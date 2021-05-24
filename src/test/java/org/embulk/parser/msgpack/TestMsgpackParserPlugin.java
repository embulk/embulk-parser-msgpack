/*
 * Copyright 2015 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.parser.msgpack;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.parser.msgpack.MsgpackParserPlugin.PluginTask;
import org.embulk.spi.FileInput;
import org.embulk.spi.FileInputRunner;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.Types;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.Pages;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMsgpackParserPlugin
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ConfigSource config;
    private Random random;
    private MsgpackParserPlugin plugin;
    private FileInputRunner runner;
    private MockPageOutput output;

    @Before
    public void createResources()
    {
        config = config().set("type", "msgpack");
        random = runtime.getRandom();
        plugin = new MsgpackParserPlugin();
        runner = new FileInputRunner(new LocalFileInputPlugin());
        output = new MockPageOutput();
    }

    @Test
    public void checkDefaultValues()
    {
        ConfigSource config = this.config.deepCopy();
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        assertEquals(MsgpackParserPlugin.FileEncoding.SEQUENCE, task.getFileEncoding());
        assertEquals(MsgpackParserPlugin.RowEncoding.MAP, task.getRowEncoding());

        // columns
        SchemaConfig schemaConfig = plugin.getSchemaConfig(task);
        assertEquals(1, schemaConfig.getColumnCount());
        assertEquals(Types.JSON, schemaConfig.getColumnType(0));
    }

    @Test(expected = ConfigException.class)
    public void throwConfigErrorByInvalidFileEncoding()
    {
        ConfigSource config = this.config.deepCopy()
                .set("columns", sampleSchema())
                .set("file_encoding", "invalid");
        CONFIG_MAPPER.map(config, PluginTask.class);
    }

    @Test(expected = ConfigException.class)
    public void throwConfigErrorByInvalidRowEncoding()
    {
        ConfigSource config = this.config.deepCopy()
                .set("columns", sampleSchema())
                .set("row_encoding", "invalid");
        CONFIG_MAPPER.map(config, PluginTask.class);
    }

    @Test(expected = ConfigException.class)
    public void throwConfigErrorIfSchemalessWithInvalidRowEncoding()
    {
        ConfigSource config = this.config.deepCopy()
                .set("row_encoding", "invalid");
        CONFIG_MAPPER.map(config, PluginTask.class);
    }

    @Test
    public void parseArrayArray()
            throws IOException
    {
        SchemaConfig schema = schema(
                column("_c_boolean", Types.BOOLEAN),
                column("_c_string", Types.STRING),
                column("_c_json", Types.JSON),
                column("_c_double", Types.DOUBLE),
                column("_c_long", Types.LONG),
                column("_c_timestamp", Types.TIMESTAMP, config().set("format", "%Y-%m-%d %H:%M:%S"))
        );
        ConfigSource config = this.config.deepCopy()
                .set("columns", schema)
                .set("file_encoding", "array")
                .set("row_encoding", "array");

        boolean vBoolean = random.nextBoolean();
        String vString = nextString(random, random.nextInt(100));
        double vDouble = random.nextDouble();
        long vLong = random.nextLong();
        String vJson = nextString(random, random.nextInt(100));
        long vTimestamp = nextUnixtime(random, "2013-01-01 00:00:00", 1000);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (MessagePacker pk = MessagePack.newDefaultPacker(out)) {
                pk.packArrayHeader(1)
                        .packArrayHeader(schema.getColumnCount()) // 1 record
                        .packBoolean(vBoolean)
                        .packString(vString)
                        .packString(vJson)
                        .packDouble(vDouble)
                        .packLong(vLong)
                        .packLong(vTimestamp);
            }

            try (FileInput in = input(out.toByteArray())) {
                transaction(config, input(out.toByteArray()), output);
            }
        }

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(1, records.size());
        for (Object[] record : records) {
            assertEquals(schema.getColumnCount(), record.length);
            assertEquals(vBoolean, record[0]);
            assertEquals(vString, record[1]);
            assertEquals(vJson, ((Value) record[2]).asStringValue().asString());
            assertEquals(vDouble, (double) record[3], 0.001);
            assertEquals(vLong, record[4]);
            assertEquals(vTimestamp, ((Timestamp) record[5]).getEpochSecond());
        }
    }

    @Test
    public void parseSequenceArray()
            throws IOException
    {
        SchemaConfig schema = schema(
                column("_c_boolean", Types.BOOLEAN),
                column("_c_string", Types.STRING),
                column("_c_json", Types.JSON),
                column("_c_double", Types.DOUBLE),
                column("_c_long", Types.LONG),
                column("_c_timestamp", Types.TIMESTAMP, config().set("format", "%Y-%m-%d %H:%M:%S"))
        );
        ConfigSource config = this.config.deepCopy()
                .set("columns", schema)
                .set("file_encoding", "sequence")
                .set("row_encoding", "array");

        boolean vBoolean = random.nextBoolean();
        String vString = nextString(random, random.nextInt(100));
        double vDouble = random.nextDouble();
        long vLong = random.nextLong();
        String vJson = nextString(random, random.nextInt(100));
        long vTimestamp = nextUnixtime(random, "2013-01-01 00:00:00", 1000);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (MessagePacker pk = MessagePack.newDefaultPacker(out)) {
                pk.packArrayHeader(schema.getColumnCount()) // 1 record
                        .packBoolean(vBoolean)
                        .packString(vString)
                        .packString(vJson)
                        .packDouble(vDouble)
                        .packLong(vLong)
                        .packLong(vTimestamp);
            }

            try (FileInput in = input(out.toByteArray())) {
                transaction(config, input(out.toByteArray()), output);
            }
        }

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(1, records.size());
        for (Object[] record : records) {
            assertEquals(schema.getColumnCount(), record.length);
            assertEquals(vBoolean, record[0]);
            assertEquals(vString, record[1]);
            assertEquals(vJson, ((Value) record[2]).asStringValue().asString());
            assertEquals(vDouble, (double) record[3], 0.001);
            assertEquals(vLong, record[4]);
            assertEquals(vTimestamp, ((Timestamp) record[5]).getEpochSecond());
        }
    }

    @Test
    public void parseSequentialSchemalessData()
            throws IOException
    {
        SchemaConfig schema = schema(column("record", Types.JSON));
        ConfigSource config = this.config.deepCopy().set("file_encoding", "sequence");

        boolean vBoolean = random.nextBoolean();
        String vString = nextString(random, random.nextInt(100));
        double vDouble = random.nextDouble();
        long vLong = random.nextLong();
        String vJson = nextString(random, random.nextInt(100));

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (MessagePacker pk = MessagePack.newDefaultPacker(out)) {
                pk.packArrayHeader(5) // 1 record
                        .packBoolean(vBoolean)
                        .packString(vString)
                        .packString(vJson)
                        .packDouble(vDouble)
                        .packLong(vLong);
            }

            try (FileInput in = input(out.toByteArray())) {
                transaction(config, input(out.toByteArray()), output);
            }
        }

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(1, records.size());
        for (Object[] record : records) {
            assertEquals(1, record.length);
            assertTrue(((Value) record[0]).isArrayValue());
            ArrayValue v = ((Value) record[0]).asArrayValue();
            assertEquals(vBoolean, v.get(0).asBooleanValue().getBoolean());
            assertEquals(vString, v.get(1).asStringValue().asString());
            assertEquals(vJson, v.get(2).asStringValue().asString());
            assertEquals(vDouble, v.get(3).asFloatValue().toDouble(), 0.001);
            assertEquals(vLong, v.get(4).asIntegerValue().toLong());
        }
    }

    @Test
    public void parseSequenceMap()
            throws IOException
    {
        SchemaConfig schema = schema(
                column("_c_boolean", Types.BOOLEAN),
                column("_c_string", Types.STRING),
                column("_c_json", Types.JSON),
                column("_c_double", Types.DOUBLE),
                column("_c_long", Types.LONG),
                column("_c_timestamp", Types.TIMESTAMP, config().set("format", "%Y-%m-%d %H:%M:%S"))
        );
        ConfigSource config = this.config.deepCopy()
                .set("columns", schema)
                .set("file_encoding", "sequence")
                .set("row_encoding", "map");

        boolean vBoolean = random.nextBoolean();
        String vString = nextString(random, random.nextInt(100));
        double vDouble = random.nextDouble();
        long vLong = random.nextLong();
        String vJson = nextString(random, random.nextInt(100));
        long vTimestamp = nextUnixtime(random, "2013-01-01 00:00:00", 1000);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (MessagePacker pk = MessagePack.newDefaultPacker(out)) {
                pk.packMapHeader(schema.getColumnCount()) // 1 record
                        .packString(schema.getColumnName(0)).packBoolean(vBoolean)
                        .packString(schema.getColumnName(1)).packString(vString)
                        .packString(schema.getColumnName(2)).packString(vJson)
                        .packString(schema.getColumnName(3)).packDouble(vDouble)
                        .packString(schema.getColumnName(4)).packLong(vLong)
                        .packString(schema.getColumnName(5)).packLong(vTimestamp);
            }

            try (FileInput in = input(out.toByteArray())) {
                transaction(config, input(out.toByteArray()), output);
            }
        }

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(1, records.size());
        for (Object[] record : records) {
            assertEquals(schema.getColumnCount(), record.length);
            assertEquals(vBoolean, record[0]);
            assertEquals(vString, record[1]);
            assertEquals(vJson, ((Value) record[2]).asStringValue().asString());
            assertEquals(vDouble, (double) record[3], 0.001);
            assertEquals(vLong, record[4]);
            assertEquals(vTimestamp, ((Timestamp) record[5]).getEpochSecond());
        }
    }

    @Test
    public void parseArrayMap()
            throws IOException
    {
        SchemaConfig schema = schema(
                column("_c_boolean", Types.BOOLEAN),
                column("_c_string", Types.STRING),
                column("_c_json", Types.JSON),
                column("_c_double", Types.DOUBLE),
                column("_c_long", Types.LONG),
                column("_c_timestamp", Types.TIMESTAMP, config().set("format", "%Y-%m-%d %H:%M:%S"))
        );
        ConfigSource config = this.config.deepCopy()
                .set("columns", schema)
                .set("file_encoding", "array")
                .set("row_encoding", "map");

        boolean vBoolean = random.nextBoolean();
        String vString = nextString(random, random.nextInt(100));
        double vDouble = random.nextDouble();
        long vLong = random.nextLong();
        String vJson = nextString(random, random.nextInt(100));
        long vTimestamp = nextUnixtime(random, "2013-01-01 00:00:00", 1000);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (MessagePacker pk = MessagePack.newDefaultPacker(out)) {
                pk.packArrayHeader(1)
                        .packMapHeader(schema.getColumnCount()) // 1 record
                        .packString(schema.getColumnName(0)).packBoolean(vBoolean)
                        .packString(schema.getColumnName(1)).packString(vString)
                        .packString(schema.getColumnName(2)).packString(vJson)
                        .packString(schema.getColumnName(3)).packDouble(vDouble)
                        .packString(schema.getColumnName(4)).packLong(vLong)
                        .packString(schema.getColumnName(5)).packLong(vTimestamp);
            }

            try (FileInput in = input(out.toByteArray())) {
                transaction(config, input(out.toByteArray()), output);
            }
        }

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(1, records.size());
        for (Object[] record : records) {
            assertEquals(schema.getColumnCount(), record.length);
            assertEquals(vBoolean, record[0]);
            assertEquals(vString, record[1]);
            assertEquals(vJson, ((Value) record[2]).asStringValue().asString());
            assertEquals(vDouble, (double) record[3], 0.001);
            assertEquals(vLong, record[4]);
            assertEquals(vTimestamp, ((Timestamp) record[5]).getEpochSecond());
        }
    }

    @Test
    public void parseArraySchemalessData()
            throws IOException
    {
        SchemaConfig schema = schema(column("record", Types.JSON));
        ConfigSource config = this.config.deepCopy().set("file_encoding", "array");

        boolean vBoolean = random.nextBoolean();
        String vString = nextString(random, random.nextInt(100));
        double vDouble = random.nextDouble();
        long vLong = random.nextLong();
        String vJson = nextString(random, random.nextInt(100));

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (MessagePacker pk = MessagePack.newDefaultPacker(out)) {
                pk.packArrayHeader(1)
                        .packArrayHeader(5) // 1 record
                        .packBoolean(vBoolean)
                        .packString(vString)
                        .packString(vJson)
                        .packDouble(vDouble)
                        .packLong(vLong);
            }

            try (FileInput in = input(out.toByteArray())) {
                transaction(config, input(out.toByteArray()), output);
            }
        }

        List<Object[]> records = Pages.toObjects(schema.toSchema(), output.pages);
        assertEquals(1, records.size());
        for (Object[] record : records) {
            assertEquals(1, record.length);
            assertTrue(((Value) record[0]).isArrayValue());
            ArrayValue v = ((Value) record[0]).asArrayValue();
            assertEquals(vBoolean, v.get(0).asBooleanValue().getBoolean());
            assertEquals(vString, v.get(1).asStringValue().asString());
            assertEquals(vJson, v.get(2).asStringValue().asString());
            assertEquals(vDouble, v.get(3).asFloatValue().toDouble(), 0.001);
            assertEquals(vLong, v.get(4).asIntegerValue().toLong());
        }
    }

    private ConfigSource config()
    {
        return runtime.getExec().newConfigSource();
    }

    private SchemaConfig sampleSchema()
    {
        return schema(column("_c0", Types.STRING));
    }

    private SchemaConfig schema(ColumnConfig... columns)
    {
        return new SchemaConfig(Lists.newArrayList(columns));
    }

    private ColumnConfig column(String name, Type type)
    {
        return column(name, type, config());
    }

    private ColumnConfig column(String name, Type type, ConfigSource config)
    {
        return new ColumnConfig(name, type, config);
    }

    private void transaction(ConfigSource config, final FileInput input, final MockPageOutput output)
    {
        plugin.transaction(config, new ParserPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema schema)
            {
                plugin.run(taskSource, schema, input, output);
            }
        });
    }

    private FileInput input(byte[] bytes)
    {
        return new InputStreamFileInput(runtime.getBufferAllocator(), provider(new ByteArrayInputStream(bytes)));
    }

    private InputStreamFileInput.IteratorProvider provider(InputStream... inputStreams)
    {
        return new InputStreamFileInput.IteratorProvider(ImmutableList.copyOf(inputStreams));
    }

    private static String nextString(Random random, int lengthBound)
    {
        char[] text = new char[lengthBound];
        for (int i = 0; i < text.length; i++) {
            text[i] = (char) random.nextInt(255);
        }
        return new String(text);
    }

    private static long nextUnixtime(Random random, String baseTime, int bound)
    {
        long baseUnixtime = java.sql.Timestamp.valueOf(baseTime).getTime();
        return baseUnixtime + random.nextInt(bound);
    }
}
