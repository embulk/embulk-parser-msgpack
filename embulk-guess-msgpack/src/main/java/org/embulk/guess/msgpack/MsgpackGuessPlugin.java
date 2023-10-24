/*
 * Copyright 2023 The Embulk project
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

package org.embulk.guess.msgpack;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.GuessPlugin;
import org.embulk.spi.json.JsonArray;
import org.embulk.spi.json.JsonObject;
import org.embulk.spi.json.JsonValue;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.guess.SchemaGuess;
import org.embulk.util.msgpack.core.MessagePack;
import org.embulk.util.msgpack.core.MessagePackException;
import org.embulk.util.msgpack.core.MessageTypeException;
import org.embulk.util.msgpack.core.MessageUnpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MsgpackGuessPlugin implements GuessPlugin {
    @Override
    public ConfigDiff guess(final ConfigSource config, final Buffer sample) {
        final ConfigDiff configDiff = CONFIG_MAPPER_FACTORY.newConfigDiff();

        if (!"msgpack".equals(config.getNestedOrGetEmpty("parser").get(String.class, "type", "msgpack"))) {
            return configDiff;
        }

        final ConfigSource parserConfig = config.getNestedOrGetEmpty("parser");
        String fileEncodingVariable = parserConfig.get(String.class, "file_encoding", null);
        String rowEncodingVariable = parserConfig.get(String.class, "row_encoding", null);

        try {
            if (fileEncodingVariable == null || rowEncodingVariable == null) {
                final MessageUnpacker uk = newUnpacker(sample);
                try {
                    uk.unpackArrayHeader();
                    try {
                        uk.unpackArrayHeader();
                        fileEncodingVariable = "array";
                        rowEncodingVariable = "array";
                    } catch (final MessageTypeException ex) {
                        fileEncodingVariable = "sequence";
                        rowEncodingVariable = "array";
                    }
                } catch (final MessageTypeException ex) {
                    final MessageUnpacker uk2 = newUnpacker(sample);  // TODO unpackArrayHeader consumes buffer (unexpectedly)
                    try {
                        final int n = uk.unpackMapHeader();
                        fileEncodingVariable = "sequence";
                        rowEncodingVariable = "map";
                    } catch (final MessageTypeException ex2) {
                        return configDiff;
                    }
                }
            }

            final String fileEncoding = fileEncodingVariable;
            final String rowEncoding = rowEncodingVariable;

            final MessageUnpacker uk = newUnpacker(sample);

            if (fileEncoding.equals("array")) {
                uk.unpackArrayHeader();  // skip array header to convert to sequence
            } else if (fileEncoding.equals("sequence")) {
                // do nothing
            }

            final List<ConfigDiff> schema;

            if (rowEncoding.equals("map")) {
                final ArrayList<LinkedHashMap<String, Object>> rows = new ArrayList<>();
                try {
                    while (true) {
                        final JsonValue value = uk.unpackAsJsonValue();
                        if (!value.isJsonObject()) {
                            throw new IllegalStateException("MessagePack map is expected, but not.");
                        }
                        rows.add(fromJsonObject(value.asJsonObject()));
                    }
                } catch (final IOException ex) {
                    // Pass through.
                }

                if (rows.size() <= 3) {
                    return configDiff;
                }

                schema = SCHEMA_GUESS.fromLinkedHashMapRecords(rows);
            } else if (rowEncoding.equals("array")) {
                final ArrayList<List<Object>> rows = new ArrayList<>();
                try {
                    while (true) {
                        final JsonValue value = uk.unpackAsJsonValue();
                        if (!value.isJsonArray()) {
                            throw new IllegalStateException("MessagePack array is expected, but not.");
                        }
                        rows.add(fromJsonArray(value.asJsonArray()));
                    }
                } catch (final IOException ex) {
                    // Pass through.
                }

                if (rows.size() <= 3) {
                    return configDiff;
                }

                final int columnCount = rows.stream().mapToInt(List::size).max().getAsInt();
                final List<String> columnNames =
                        IntStream.range(0, columnCount).mapToObj(i -> "c" + i).collect(Collectors.toList());
                schema = SCHEMA_GUESS.fromListRecords(columnNames, rows);
            } else {
                schema = null;
            }

            final ConfigDiff parserGuessed = CONFIG_MAPPER_FACTORY.newConfigDiff();
            parserGuessed.set("type", "msgpack");
            parserGuessed.set("row_encoding", rowEncoding);
            parserGuessed.set("file_encoding", fileEncoding);
            if (schema != null) {
                parserGuessed.set("columns", schema);
            }
            configDiff.setNested("parser", parserGuessed);
        } catch (final MessagePackException | IllegalStateException | IOException ex) {
            logger.warn("Failed in parsing the input as MessagePack.", ex);
            return configDiff;
        }

        return configDiff;
    }

    private static MessageUnpacker newUnpacker(final Buffer sample) {
        return MessagePack.newDefaultUnpacker(sample.array());
    }

    private static Object fromJsonValue(final JsonValue value) {
        switch (value.getEntityType()) {
            case NULL:
                return null;
            case BOOLEAN:
                return value.asJsonBoolean().booleanValue();
            case LONG:
                return value.asJsonLong().longValue();
            case DOUBLE:
                return value.asJsonDouble().doubleValue();
            case STRING:
                return value.asJsonString().getString();
            case ARRAY:
                return fromJsonArray(value.asJsonArray());
            case OBJECT:
                return fromJsonObject(value.asJsonObject());
            default:
                throw new IllegalStateException();
        }
    }

    private static ArrayList<Object> fromJsonArray(final JsonArray array) {
        final ArrayList<Object> list = new ArrayList<>();
        for (final JsonValue element : array) {
            list.add(fromJsonValue(element));
        }
        return list;
    }

    private static LinkedHashMap<String, Object> fromJsonObject(final JsonObject object) {
        final LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        for (final Map.Entry<String, JsonValue> entry : object.entrySet()) {
            map.put(entry.getKey(), fromJsonValue(entry.getValue()));
        }
        return map;
    }

    private static final Logger logger = LoggerFactory.getLogger(MsgpackGuessPlugin.class);

    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();

    private static final SchemaGuess SCHEMA_GUESS = SchemaGuess.of(CONFIG_MAPPER_FACTORY);
}
