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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.Comparator;
import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.embulk.spi.Exec;
import org.embulk.spi.type.Types;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.MessageInsufficientBufferException;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.core.buffer.MessageBufferInput;
import org.msgpack.value.Value;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.Type;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.JsonType;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.dynamic.BooleanColumnSetter;
import org.embulk.util.dynamic.DefaultValueSetter;
import org.embulk.util.dynamic.DoubleColumnSetter;
import org.embulk.util.dynamic.DynamicColumnSetter;
import org.embulk.util.dynamic.JsonColumnSetter;
import org.embulk.util.dynamic.LongColumnSetter;
import org.embulk.util.dynamic.NullDefaultValueSetter;
import org.embulk.util.dynamic.StringColumnSetter;
import org.embulk.util.dynamic.TimestampColumnSetter;
import org.embulk.util.timestamp.TimestampFormatter;

public class MsgpackParserPlugin
        implements ParserPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("file_encoding")
        @ConfigDefault("\"sequence\"")
        public FileEncoding getFileEncoding();

        @Config("row_encoding")
        @ConfigDefault("\"map\"")
        public RowEncoding getRowEncoding();

        @Config("columns")
        @ConfigDefault("null")
        public Optional<SchemaConfig> getSchemaConfig();

        // From org.embulk.spi.time.TimestampParser.Task
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public String getDefaultTimeZoneId();

        // From org.embulk.spi.time.TimestampParser.Task
        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N %z\"")
        public String getDefaultTimestampFormat();

        // From org.embulk.spi.time.TimestampParser.Task
        @Config("default_date")
        @ConfigDefault("\"1970-01-01\"")
        public String getDefaultDate();

        public void setSchemafulMode(boolean v);
        public boolean getSchemafulMode();
    }

    // From org.embulk.spi.time.TimestampParser.TimestampColumnOption
    public interface TimestampColumnOptionForParsing
            extends Task
    {
        @Config("timezone")
        @ConfigDefault("null")
        public Optional<String> getTimeZoneId();

        @Config("format")
        @ConfigDefault("null")
        public Optional<String> getFormat();

        @Config("date")
        @ConfigDefault("null")
        public Optional<String> getDate();
    }

    public static enum FileEncoding
    {
        SEQUENCE("sequence"),
        ARRAY("array");

        private final String name;

        private FileEncoding(String name)
        {
            this.name = name;
        }

        @JsonCreator
        public static FileEncoding of(String name)
        {
            for (FileEncoding enc : FileEncoding.values()) {
                if (enc.toString().equals(name)) {
                    return enc;
                }
            }
            throw new ConfigException(String.format("Invalid FileEncoding '%s'. Available options are sequence or array", name));
        }

        @JsonValue
        @Override
        public String toString()
        {
            return name;
        }
    }

    public static enum RowEncoding
    {
        ARRAY("array"),
        MAP("map");

        private final String name;

        private RowEncoding(String name)
        {
            this.name = name;
        }

        @JsonCreator
        public static RowEncoding of(String name)
        {
            for (RowEncoding enc : RowEncoding.values()) {
                if (enc.toString().equals(name)) {
                    return enc;
                }
            }
            if ("object".equals(name)) {
                // alias of map
                return MAP;
            }
            throw new ConfigException(String.format("Invalid RowEncoding '%s'. Available options are array or map", name));
        }

        @JsonValue
        @Override
        public String toString()
        {
            return name;
        }
    }

    public interface PluginTaskFormatter
            extends Task
    {
        // From org.embulk.spi.time.TimestampFormatter.Task
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public String getDefaultTimeZoneId();

        // From org.embulk.spi.time.TimestampFormatter.Task
        @Config("default_timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%6N %z\"")
        public String getDefaultTimestampFormat();
    }

    // From org.embulk.spi.time.TimestampFormatter.TimestampColumnOption
    public interface TimestampColumnOptionForFormatting
            extends Task
    {
        @Config("timezone")
        @ConfigDefault("null")
        public Optional<String> getTimeZoneId();

        @Config("format")
        @ConfigDefault("null")
        public Optional<String> getFormat();
    }

    private static class FileInputMessageBufferInput
            implements MessageBufferInput
    {
        private final FileInput input;
        private Buffer lastBuffer = null;

        public FileInputMessageBufferInput(FileInput input)
        {
            this.input = input;
        }

        @Override
        public MessageBuffer next()
        {
            Buffer b = input.poll();
            if (lastBuffer != null) {
                lastBuffer.release();
            }
            lastBuffer = b;
            if (b == null) {
                return null;
            }
            else {
                return MessageBuffer.wrap(b.array()).slice(b.offset(), b.limit());
            }
        }

        @Override
        public void close()
        {
            if (lastBuffer != null) {
                lastBuffer.release();
            }
            input.close();
        }
    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        if (!task.getSchemaConfig().isPresent()) {
            // If columns: is not set, the parser behaves as non-schemaful mode. It doesn't care of row encoding.
            if (config.has("row_encoding")) {
                throw new ConfigException("Setting row_encoding: is invalid if columns: is not set.");
            }
            task.setSchemafulMode(false);
        }
        else {
            task.setSchemafulMode(true);
        }

        control.run(task.dump(), getSchemaConfig(task).toSchema());
    }

    SchemaConfig getSchemaConfig(PluginTask task)
    {
        Optional<SchemaConfig> schemaConfig = task.getSchemaConfig();
        if (schemaConfig.isPresent()) {
            return schemaConfig.get();
        }
        else {
            final ArrayList<ColumnConfig> columnConfigs = new ArrayList<>();
            columnConfigs.add(new ColumnConfig("record", Types.JSON, CONFIG_MAPPER_FACTORY.newConfigSource()));
            return new SchemaConfig(Collections.unmodifiableList(columnConfigs));
        }
    }

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        boolean schemafulMode = task.getSchemafulMode();
        FileEncoding fileEncoding = task.getFileEncoding();

        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputMessageBufferInput(input));
                final PageBuilder pageBuilder = Exec.getPageBuilder(Exec.getBufferAllocator(), schema, output)) {

            if (schemafulMode) {
                RowEncoding rowEncoding = task.getRowEncoding();
                final TimestampFormatter[] timestampFormattersForParsing =
                        newTimestampColumnFormattersForParsing(task, getSchemaConfig(task));
                Map<Column, DynamicColumnSetter> setters = newColumnSetters(pageBuilder,
                        getSchemaConfig(task), timestampFormattersForParsing, TASK_MAPPER.map(taskSource, PluginTaskFormatter.class));

                RowReader reader;
                switch (rowEncoding) {
                case ARRAY:
                    reader = new ArrayRowReader(setters);
                    break;
                case MAP:
                    reader = new MapRowReader(setters);
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected row encoding");
                }

                while (input.nextFile()) {
                    switch (fileEncoding) {
                    case SEQUENCE:
                        // do nothing
                        break;
                    case ARRAY:
                        // skip array header to convert array to sequence
                        unpacker.unpackArrayHeader();
                        break;
                    }

                    while (reader.next(unpacker)) {
                        pageBuilder.addRecord();
                    }
                }
            }
            else {
                // If non-schemaful mode, setters is not created.
                while (input.nextFile()) {
                    switch (fileEncoding) {
                    case SEQUENCE:
                        // do nothing
                        break;
                    case ARRAY:
                        // skip array header to convert array to sequence
                        unpacker.unpackArrayHeader();
                        break;
                    }

                    while (true) {
                        Value v;
                        try {
                            v = unpacker.unpackValue();
                            if (v == null) {
                                break;
                            }
                        }
                        catch (MessageInsufficientBufferException e) {
                            break;
                        }

                        // The unpacked Value object is set to a page as a Json column value.
                        pageBuilder.setJson(0, v);
                        pageBuilder.addRecord();
                    }
                }
            }

            pageBuilder.finish();

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static TimestampFormatter[] newTimestampColumnFormattersForParsing(final PluginTask task, final SchemaConfig schema) {
        final TimestampFormatter[] formatters = new TimestampFormatter[schema.getColumnCount()];
        int i = 0;
        for (final ColumnConfig column : schema.getColumns()) {
            if (column.getType() instanceof TimestampType) {
                final TimestampColumnOptionForParsing columnOption = CONFIG_MAPPER.map(column.getOption(), TimestampColumnOptionForParsing.class);

                final String pattern = columnOption.getFormat().orElse(task.getDefaultTimestampFormat());
                formatters[i] = TimestampFormatter.builder(pattern, true)
                        .setDefaultZoneFromString(columnOption.getTimeZoneId().orElse(task.getDefaultTimeZoneId()))
                        .setDefaultDateFromString(columnOption.getDate().orElse(task.getDefaultDate()))
                        .build();
            }
            i++;
        }
        return formatters;
    }

    public static TimestampFormatter newTimestampFormatterForFormatting(
            final PluginTaskFormatter formatterTask, final ColumnConfig c) {
        final TimestampColumnOptionForFormatting columnOption =
                CONFIG_MAPPER.map(c.getOption(), TimestampColumnOptionForFormatting.class);

        final String pattern = columnOption.getFormat().orElse(formatterTask.getDefaultTimestampFormat());

        return TimestampFormatter.builder(pattern, true)
                .setDefaultZoneFromString(columnOption.getTimeZoneId().orElse(formatterTask.getDefaultTimeZoneId()))
                .build();
    }

    private Map<Column, DynamicColumnSetter> newColumnSetters(PageBuilder pageBuilder,
            SchemaConfig schema, TimestampFormatter[] timestampFormattersForParsing, PluginTaskFormatter formatterTask)
    {
        final LinkedHashMap<Column, DynamicColumnSetter> builder = new LinkedHashMap<>();
        int index = 0;
        for (ColumnConfig c : schema.getColumns()) {
            Column column = c.toColumn(index);
            Type type = column.getType();

            DefaultValueSetter defaultValue = new NullDefaultValueSetter();
            DynamicColumnSetter setter;

            if (type instanceof BooleanType) {
                setter = new BooleanColumnSetter(pageBuilder, column, defaultValue);

            }
            else if (type instanceof LongType) {
                setter = new LongColumnSetter(pageBuilder, column, defaultValue);

            }
            else if (type instanceof DoubleType) {
                setter = new DoubleColumnSetter(pageBuilder, column, defaultValue);

            }
            else if (type instanceof StringType) {
                final TimestampFormatter formatter = newTimestampFormatterForFormatting(formatterTask, c);
                setter = new StringColumnSetter(pageBuilder, column, defaultValue, formatter);

            }
            else if (type instanceof TimestampType) {
                final TimestampFormatter formatterForParsing = timestampFormattersForParsing[column.getIndex()];
                setter = new TimestampColumnSetter(pageBuilder, column, defaultValue, formatterForParsing);

            }
            else if (type instanceof JsonType) {
                final TimestampFormatter formatter = newTimestampFormatterForFormatting(formatterTask, c);
                setter = new JsonColumnSetter(pageBuilder, column, defaultValue, formatter);

            }
            else {
                throw new ConfigException("Unknown column type: " + type);
            }

            builder.put(column, setter);
            index++;
        }
        return Collections.unmodifiableMap(builder);
    }

    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);

    static void unpackToSetter(MessageUnpacker unpacker, DynamicColumnSetter setter)
            throws IOException
    {
        MessageFormat format = unpacker.getNextFormat();
        switch (format.getValueType()) {
        case NIL:
            unpacker.unpackNil();
            setter.setNull();
            break;

        case BOOLEAN:
            setter.set(unpacker.unpackBoolean());
            break;

        case INTEGER:
            if (format == MessageFormat.UINT64) {
                BigInteger bi = unpacker.unpackBigInteger();
                if (0 <= bi.compareTo(LONG_MIN) && bi.compareTo(LONG_MAX) <= 0) {
                    setter.set(bi.longValue());
                }
                else {
                    setter.setNull();  // TODO set default value
                }
            }
            else {
                setter.set(unpacker.unpackLong());
            }
            break;

        case FLOAT:
            setter.set(unpacker.unpackDouble());
            break;

        case STRING:
            setter.set(unpacker.unpackString());
            break;

        case BINARY:
            setter.set(unpacker.unpackString());
            break;

        case ARRAY:
        case MAP:
            setter.set(unpacker.unpackValue());
            break;

        case EXTENSION:
            unpacker.skipValue();
            setter.setNull();
            break;
        }
    }

    private interface RowReader
    {
        public boolean next(MessageUnpacker unpacker) throws IOException;
    }

    private class ArrayRowReader
            implements RowReader
    {
        private final DynamicColumnSetter[] columnSetters;

        public ArrayRowReader(Map<Column, DynamicColumnSetter> setters)
        {
            this.columnSetters = new DynamicColumnSetter[setters.size()];
            for (Map.Entry<Column, DynamicColumnSetter> pair : setters.entrySet()) {
                columnSetters[pair.getKey().getIndex()] = pair.getValue();
            }
        }

        public boolean next(MessageUnpacker unpacker) throws IOException
        {
            int n;
            try {
                n = unpacker.unpackArrayHeader();
            }
            catch (MessageInsufficientBufferException ex) {
                // TODO EOFException?
                return false;
            }
            for (int i = 0; i < n; i++) {
                if (i < columnSetters.length) {
                    unpackToSetter(unpacker, columnSetters[i]);
                }
                else {
                    unpacker.skipValue();
                }
            }
            return true;
        }
    }

    private class MapRowReader
            implements RowReader
    {
        private final Map<String, DynamicColumnSetter> columnSetters;

        public MapRowReader(Map<Column, DynamicColumnSetter> setters)
        {
            this.columnSetters = new TreeMap<>();
            for (Map.Entry<Column, DynamicColumnSetter> pair : setters.entrySet()) {
                columnSetters.put(pair.getKey().getName(), pair.getValue());
            }
        }

        public boolean next(MessageUnpacker unpacker) throws IOException
        {
            int n;
            try {
                n = unpacker.unpackMapHeader();
            }
            catch (MessageInsufficientBufferException ex) {
                // TODO EOFException?
                return false;
            }
            for (int i = 0; i < n; i++) {
                MessageFormat format = unpacker.getNextFormat();
                if (!format.getValueType().isRawType()) {
                    unpacker.skipValue();
                    continue;
                }
                // TODO optimize
                //MessageBuffer key = unpacker.readPayloadAsReference(unpacker.unpackRawStringHeader());
                String key = new String(unpacker.readPayload(unpacker.unpackRawStringHeader()));
                DynamicColumnSetter setter = columnSetters.get(key);
                if (setter != null) {
                    unpackToSetter(unpacker, setter);
                }
                else {
                    unpacker.skipValue();
                }
            }
            return true;
        }
    }

    private static class MessageBufferEqualComparator
            implements Comparator<MessageBuffer>
    {
        @Override
        public int compare(MessageBuffer o1, MessageBuffer o2)
        {
            if (o1.size() == o2.size()) {
                int offset = 0;
                int length = o1.size();
                while (length - offset > 8) {
                    long a = o1.getLong(offset);
                    long b = o2.getLong(offset);
                    if (a != b) {
                        return (int) (a - b);
                    }
                    offset += 8;
                }
                while (length - offset > 0) {
                    byte a = o1.getByte(offset);
                    byte b = o2.getByte(offset);
                    if (a != b) {
                        return a - b;
                    }
                    offset += 1;
                }
                return 0;
            }
            else {
                return o1.size() - o2.size();
            }
        }
    }

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
    private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    private static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();
}
