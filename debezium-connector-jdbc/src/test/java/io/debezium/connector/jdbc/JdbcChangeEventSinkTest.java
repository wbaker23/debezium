package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.connect.sink.SinkRecord;
import org.hibernate.StatelessSession;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.jdbc.ReturningWork;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.connector.jdbc.util.DebeziumSinkRecordFactory;
import io.debezium.connector.jdbc.util.FlatSinkRecordFactory;
import io.debezium.connector.jdbc.util.SinkRecordFactory;
import io.debezium.metadata.CollectionId;
import io.debezium.openlineage.ConnectorContext;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;

@Tag("UnitTests")
class JdbcChangeEventSinkTest {

    private static final String TOPIC = "foo";
    private static final CollectionId TABLE_ID = new CollectionId(TOPIC);

    static Stream<Arguments> recordFormatsAndBufferModes() {
        return Stream.of(
                Arguments.of(new DebeziumSinkRecordFactory(), false),
                Arguments.of(new DebeziumSinkRecordFactory(), true),
                Arguments.of(new FlatSinkRecordFactory(), false),
                Arguments.of(new FlatSinkRecordFactory(), true));
    }

    @ParameterizedTest
    @MethodSource("recordFormatsAndBufferModes")
    void shouldReuseTableMetadataAcrossExecuteCalls(SinkRecordFactory factory, boolean reduction) throws Exception {
        try (var lineage = mockStatic(DebeziumOpenLineageEmitter.class)) {
            final var fixture = new Fixture(reduction);

            fixture.sink.execute(List.of(record(factory, 1)));

            verify(fixture.dialect, times(2)).tableExists(fixture.connection, TABLE_ID);
            verify(fixture.dialect, times(1)).readTable(fixture.connection, TABLE_ID);
            assertThat(fixture.writtenIds()).containsExactly((byte) 1);

            fixture.sink.execute(List.of(record(factory, 2)));

            verify(fixture.dialect, times(2)).tableExists(fixture.connection, TABLE_ID);
            verify(fixture.dialect, times(1)).readTable(fixture.connection, TABLE_ID);
            assertThat(fixture.writtenIds()).containsExactly((byte) 1, (byte) 2);
            verify(fixture.writer, times(2)).write(anyList(), any());
        }
    }

    @ParameterizedTest
    @MethodSource("recordFormatsAndBufferModes")
    void shouldFlushEveryRecordWithoutReplayingPreviousBatches(SinkRecordFactory factory, boolean reduction) throws Exception {
        try (var lineage = mockStatic(DebeziumOpenLineageEmitter.class)) {
            final var fixture = new Fixture(reduction);

            fixture.sink.execute(List.of(
                    record(factory, 1),
                    record(factory, 2)));

            assertThat(fixture.writtenIds()).containsExactlyInAnyOrder((byte) 1, (byte) 2);
            assertThat(fixture.writes).hasSize(1);

            fixture.sink.execute(List.of(
                    record(factory, 3),
                    record(factory, 4),
                    record(factory, 5),
                    record(factory, 6)));

            assertThat(fixture.writtenIds()).containsExactlyInAnyOrder(
                    (byte) 1, (byte) 2, (byte) 3,
                    (byte) 4, (byte) 5, (byte) 6);

            // NB: batch.size=3
            assertThat(fixture.writes)
                    .extracting(List::size)
                    .containsExactly(2, 3, 1);

            fixture.sink.execute(List.of());

            assertThat(fixture.writtenIds()).containsExactlyInAnyOrder(
                    (byte) 1, (byte) 2, (byte) 3,
                    (byte) 4, (byte) 5, (byte) 6);

            verify(fixture.writer, times(3)).write(anyList(), any());
        }
    }

    private static SinkRecord record(SinkRecordFactory factory, int id) {
        return factory.createRecord(TOPIC, (byte) id).getOriginalKafkaRecord();
    }

    private static class Fixture {
        private final Connection connection = mock(Connection.class);
        private final StatelessSession session = mock(StatelessSession.class);
        private final DatabaseDialect dialect = mock(DatabaseDialect.class);
        private final RecordWriter writer = mock(RecordWriter.class);
        private final List<List<JdbcSinkRecord>> writes = new ArrayList<>();
        private final JdbcChangeEventSink sink;

        private Fixture(boolean reduction) throws Exception {
            final var config = new JdbcSinkConnectorConfig(Map.of(
                    "connection.url", "jdbc:db2://unused:50000/test",
                    "insert.mode", "insert",
                    "primary.key.mode", "record_key",
                    "batch.size", "3",
                    "use.reduction.buffer", Boolean.toString(reduction)));

            final var table = TableDescriptor.builder()
                    .tableName(TOPIC)
                    .type("TABLE")
                    .build();

            final var jdbcType = mock(JdbcType.class);

            when(jdbcType.getTypeName(any(), anyBoolean())).thenReturn("VARCHAR");
            when(dialect.getVersion()).thenReturn(DatabaseVersion.make(11, 5));
            when(dialect.getSchemaType(any())).thenReturn(jdbcType);
            when(dialect.getCollectionId(TOPIC)).thenReturn(TABLE_ID);
            when(dialect.tableExists(connection, TABLE_ID)).thenReturn(true);
            when(dialect.readTable(connection, TABLE_ID)).thenReturn(table);
            when(dialect.resolveMissingFields(any(), eq(table))).thenReturn(Set.of());
            when(dialect.getBatchInsertStatement(eq(table), anyList())).thenReturn(Optional.empty());
            when(dialect.getInsertStatement(eq(table), any())).thenReturn("INSERT INTO customers VALUES (?, ?, ?)");

            doAnswer(invocation -> {
                final ReturningWork<?> work = invocation.getArgument(0);
                return work.execute(connection);
            }).when(session).doReturningWork(any());

            doAnswer(invocation -> {
                final List<JdbcSinkRecord> records = invocation.getArgument(0);
                writes.add(List.copyOf(records));
                return null;
            }).when(writer).write(anyList(), any());

            sink = new JdbcChangeEventSink(
                    config,
                    session,
                    dialect,
                    writer,
                    mock(ConnectorContext.class));
        }

        private List<Byte> writtenIds() {
            return writes.stream()
                    .flatMap(List::stream)
                    .map(record -> record.filteredKey().getInt8("id"))
                    .toList();
        }
    }
}