/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.*;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

@ExtendWith(ParameterizedTestExtension.class)
public class TestValidation extends CatalogTestBase {

    public static final Schema SCHEMA =
            new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.required(2, "data", Types.StringType.get()));
    private static final Map<String, String> CATALOG_PROPS =
            ImmutableMap.of(
                    "type", "hive",
                    "default-namespace", "default",
                    "cache-enabled", "false");

    @Parameter(index = 3)
    private FileFormat format;

    @Parameters(name = "catalogName = {1}, implementation = {2}, config = {3}, fileFormat = {4}")
    public static Object[][] parameters() {
        return new Object[][] {
                {
                        SparkCatalogConfig.HIVE.catalogName(),
                        SparkCatalogConfig.HIVE.implementation(),
                        CATALOG_PROPS,
                        FileFormat.PARQUET
                },
                {
                        SparkCatalogConfig.HIVE.catalogName(),
                        SparkCatalogConfig.HIVE.implementation(),
                        CATALOG_PROPS,
                        FileFormat.AVRO
                },
                {
                        SparkCatalogConfig.HIVE.catalogName(),
                        SparkCatalogConfig.HIVE.implementation(),
                        CATALOG_PROPS,
                        FileFormat.ORC
                },
        };
    }

    @TestTemplate
    public void testConflictingUpdateAndDeleteWithValidation() throws Exception {
        String tableName = "write_null_rows";
        Table tab = createTable(tableName, SCHEMA, PartitionSpec.unpartitioned());

        // Append a row [1, "apple"]
        DataFile dFile = dataFile(tab, 1, "apple");
        tab.newAppend().appendFile(dFile).commit();
        long snapshotId = tab.currentSnapshot().snapshotId();

        // Update row [1, "apple"] to [1, "banana"]
        DataFile dFile2 = dataFile(tab, 1, "banana");
        List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
        deletes.add(Pair.of(dFile.path(), 0L));
        Pair<DeleteFile, CharSequenceSet> posDeletes =
                FileHelpers.writeDeleteFile(
                        tab,
                        Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
                        TestHelpers.Row.of(0),
                        deletes);
        tab.newRowDelta().addRows(dFile2).addDeletes(posDeletes.first()).commit();

        // Delete row [1, "apple"], validating from the snapshot prior to
        // the update commit.
        List<Pair<CharSequence, Long>> deletes2 = Lists.newArrayList();
        deletes2.add(Pair.of(dFile.path(), 0L));
        Pair<DeleteFile, CharSequenceSet> posDeletes2 =
                FileHelpers.writeDeleteFile(
                        tab,
                        Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
                        TestHelpers.Row.of(0),
                        deletes2);

        // the configured validation rules should catch the conflict and prevent the
        // delete from successfully committing.
        assertThatThrownBy(() -> tab.newRowDelta().validateFromSnapshot(snapshotId)
                .validateDeletedFiles()
                .validateNoConflictingDeleteFiles()
                .addDeletes(posDeletes2.first())
                .commit())
                .isInstanceOf(ValidationException.class);

        dropTable(tableName);
    }

    @TestTemplate
    public void testConflictingUpdateAndDeleteWithoutValidation() throws Exception {
        String tableName = "write_null_rows";
        Table tab = createTable(tableName, SCHEMA, PartitionSpec.unpartitioned());

        // Append a row [1, "apple"]
        DataFile dFile = dataFile(tab, 1, "apple");
        tab.newAppend().appendFile(dFile).commit();
        long snapshotId = tab.currentSnapshot().snapshotId();

        // Update row [1, "apple"] to [1, "banana"]
        DataFile dFile2 = dataFile(tab, 1, "banana");
        List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
        deletes.add(Pair.of(dFile.path(), 0L));
        Pair<DeleteFile, CharSequenceSet> posDeletes =
                FileHelpers.writeDeleteFile(
                        tab,
                        Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
                        TestHelpers.Row.of(0),
                        deletes);
        tab.newRowDelta().addRows(dFile2).addDeletes(posDeletes.first()).commit();

        // Delete row [1, "apple"], validating from the snapshot prior to
        // the update commit.
        List<Pair<CharSequence, Long>> deletes2 = Lists.newArrayList();
        deletes2.add(Pair.of(dFile.path(), 0L));
        Pair<DeleteFile, CharSequenceSet> posDeletes2 =
                FileHelpers.writeDeleteFile(
                        tab,
                        Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
                        TestHelpers.Row.of(0),
                        deletes2);

        assertThatCode(() -> tab.newRowDelta().validateFromSnapshot(snapshotId)
                .addDeletes(posDeletes2.first())
                .commit())
                .doesNotThrowAnyException();

        Dataset<Row> scanDF =
                spark
                        .read()
                        .format("iceberg")
                        .load(tableName);
        scanDF.foreach((ForeachFunction<Row>) row -> System.out.println(row));
        // due to the lack of validation, we expect the delete to succeed but not delete
        // the new version of row with id=1.
        assertThat(scanDF.count() == 1L);

        dropTable(tableName);
    }

    protected Table createTable(String name, Schema schema, PartitionSpec spec) {
        Map<String, String> properties =
                ImmutableMap.of(
                        TableProperties.FORMAT_VERSION,
                        "2",
                        TableProperties.DEFAULT_FILE_FORMAT,
                        format.toString());
        return validationCatalog.createTable(
                TableIdentifier.of("default", name), schema, spec, properties);
    }

    protected void dropTable(String name) {
        validationCatalog.dropTable(TableIdentifier.of("default", name), false);
    }

    private DataFile dataFile(Table tab, Integer idValue, String dataValue, Object... partValues) throws IOException {
        return dataFile(tab, idValue, dataValue, partValues, partValues);
    }

    private DataFile dataFile(Table tab, Integer idValue, String dataValue, Object[] partDataValues, Object[] partFieldValues)
            throws IOException {
        GenericRecord record = GenericRecord.create(tab.schema());
        List<String> partitionFieldNames =
                tab.spec().fields().stream().map(PartitionField::name).collect(Collectors.toList());
        List<Record> records =
                Lists.newArrayList(
                        record.copy(
                                "id",
                                idValue,
                                "data",
                                dataValue));

        // fill remaining columns with incremental values
        List<Types.NestedField> cols = tab.schema().columns();
        if (cols.size() > 2) {
            for (int i = 2; i < cols.size(); i++) {
                final int pos = i;
                records.forEach(r -> r.set(pos, pos));
            }
        }

        TestHelpers.Row partitionInfo = TestHelpers.Row.of(partFieldValues);
        return FileHelpers.writeDataFile(
                tab,
                Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
                partitionInfo,
                records);
    }
}