package com.gongyu.alinkple.ex;


import com.alibaba.alink.common.AlinkGlobalConfiguration;
import com.alibaba.alink.common.io.catalog.MySqlCatalog;
import com.alibaba.alink.common.io.plugin.PluginDownloader;
import com.alibaba.alink.common.utils.JsonConverter;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.sink.CatalogSinkBatchOp;
import com.alibaba.alink.operator.batch.source.CatalogSourceBatchOp;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.operator.batch.source.MemSourceBatchOp;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.sink.CatalogSinkStreamOp;
import com.alibaba.alink.operator.stream.source.CatalogSourceStreamOp;
import com.alibaba.alink.operator.stream.source.CsvSourceStreamOp;
import com.alibaba.alink.params.io.HasCatalogObject;
import com.alibaba.alink.pipeline.clustering.KMeans;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Catalog {
//    static final String DATA_DIR = Utils.ROOT_DIR + "db" + File.separator;

    static final String ALINK_PLUGIN_DIR = "D:/alink/alink_plugin";

    static final String IRIS_URL =
            "http://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data";
    static final String IRIS_SCHEMA_STR =
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";

    static final String DB_NAME = "test_db";
    static final String BATCH_TABLE_NAME = "batch_table";
    static final String STREAM_TABLE_NAME = "stream_table";

    static final String HIVE_VERSION = "2.3.4";
    static final String HIVE_CONF_DIR = null;

    static final String DERBY_VERSION = "10.6.1.0";
    static final String DERBY_DIR = "derby";

    static final String MYSQL_VERSION = "5.1.27";
    static final String MYSQL_URL = "192.168.96.96";
    static final String MYSQL_PORT = "33019";
    static final String MYSQL_USER_NAME = "root";
    static final String MYSQL_PASSWORD = "root";

    static PluginDownloader DOWNLOADER;

    public static void main(String[] args) throws Exception {
        AlinkGlobalConfiguration.setPluginDir(ALINK_PLUGIN_DIR);

        AlinkGlobalConfiguration.setPrintProcessInfo(true);
        DOWNLOADER = AlinkGlobalConfiguration.getPluginDownloader();
        DOWNLOADER.downloadPlugin("mysql", MYSQL_VERSION);
        c_5();
    }

    static void c_4() throws Exception {

        if (null != MYSQL_URL) {
            MySqlCatalog mySql = new MySqlCatalog("mysql_catalog", "default", MYSQL_VERSION,
                    MYSQL_URL, MYSQL_PORT, MYSQL_USER_NAME, MYSQL_PASSWORD);
            mySql.open();
//            mySql.createDatabase(DB_NAME, new CatalogDatabaseImpl(new HashMap<>(), ""), true);

            // 批处理读取csv文件 数据表导出
//            new CsvSourceBatchOp()
//                    .setFilePath(IRIS_URL)
//                    .setSchemaStr(IRIS_SCHEMA_STR)
//                    .lazyPrintStatistics("< origin data >")
//                    .link(
//                            new CatalogSinkBatchOp()
//                                    .setCatalogObject(new HasCatalogObject.CatalogObject(mySql, new ObjectPath(DB_NAME, BATCH_TABLE_NAME)))
//                    );
//            BatchOperator.execute();

            // 流处理读取csv文件 数据表导出
//            new CsvSourceStreamOp()
//                    .setFilePath(IRIS_URL)
//                    .setSchemaStr(IRIS_SCHEMA_STR)
//                    .link(
//                            new CatalogSinkStreamOp()
//                                    .setCatalogObject(new HasCatalogObject.CatalogObject(mySql, new ObjectPath(DB_NAME, STREAM_TABLE_NAME)))
//                    );
//            StreamOperator.execute();


//            new CatalogSourceBatchOp()
//                    .setCatalogObject(new HasCatalogObject.CatalogObject(mySql, new ObjectPath(DB_NAME, BATCH_TABLE_NAME)))
//                    .lazyPrintStatistics("< batch catalog source >");
//            BatchOperator.execute();

            new CatalogSourceStreamOp()
                    .setCatalogObject(new HasCatalogObject.CatalogObject(mySql, new ObjectPath(DB_NAME, STREAM_TABLE_NAME)))
                    .sample(0.02)
                    .print();
            StreamOperator.execute();

            System.out.println("< tables before drop >");
            System.out.println(JsonConverter.toJson(mySql.listTables(DB_NAME)));

            if (mySql.tableExists(new ObjectPath(DB_NAME, BATCH_TABLE_NAME))) {
                mySql.dropTable(new ObjectPath(DB_NAME, BATCH_TABLE_NAME), false);
            }
            mySql.dropTable(new ObjectPath(DB_NAME, STREAM_TABLE_NAME), true);

            System.out.println("< tables after drop >");
            System.out.println(JsonConverter.toJson(mySql.listTables(DB_NAME)));

            mySql.dropDatabase(DB_NAME, true);
            mySql.close();
        }
    }

    static void c_5() throws Exception {
        if (null != MYSQL_URL) {
            MySqlCatalog mySql = new MySqlCatalog("mysql_catalog", "default", MYSQL_VERSION,
                    MYSQL_URL, MYSQL_PORT, MYSQL_USER_NAME, MYSQL_PASSWORD);
            mySql.open();
//            mySql.createDatabase(DB_NAME, new CatalogDatabaseImpl(new HashMap<>(), ""), true);
//            List <Row> df = Arrays.asList(
//                    Row.of(0, "0 0 0"),
//                    Row.of(1, "0.1,0.1,0.1"),
//                    Row.of(2, "0.2,0.2,0.2"),
//                    Row.of(3, "9 9 9"),
//                    Row.of(4, "9.1 9.1 9.1"),
//                    Row.of(5, "9.2 9.2 9.2")
//            );
//            BatchOperator <?> inOp = new MemSourceBatchOp(df, "id int, vec string");
//            new CsvSourceBatchOp()
//                    .setFilePath(IRIS_URL)
//                    .setSchemaStr(IRIS_SCHEMA_STR)
//                    .lazyPrintStatistics("< origin data >")
//                    .link(
//                            new CatalogSinkBatchOp()
//                                    .setCatalogObject(new HasCatalogObject.CatalogObject(mySql, new ObjectPath(DB_NAME, BATCH_TABLE_NAME)))
//                    );
//            BatchOperator.execute();
            CatalogSourceBatchOp catalogSourceBatchOp = new CatalogSourceBatchOp()
                    .setCatalogObject(new HasCatalogObject.CatalogObject(mySql, new ObjectPath(DB_NAME, BATCH_TABLE_NAME)));

            KMeans kmeans = new KMeans()
                    .setVectorCol("sepal_length")
                    .setK(2)
                    .setPredictionCol("pred");
            BatchOperator<?> transform = kmeans.fit(catalogSourceBatchOp).transform(catalogSourceBatchOp);
            new CatalogSinkBatchOp()
                    .setCatalogObject(new HasCatalogObject.CatalogObject(mySql, new ObjectPath(DB_NAME, "test1")))
                    .linkFrom(transform);

            BatchOperator.execute();
        }
    }
}