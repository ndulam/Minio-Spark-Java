package org.cloud.data;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class IcebergTable {
    public static void main(String[] args) throws NoSuchTableException {

        System.out.println("Hello World");
        System.setProperty("hadoop.home.dir", "D:\\sparksetup\\hadoop");
        System.setProperty("aws.region", "us-east-1");
        String s3accessKeyAws = "minio_access_key";
        String s3secretKeyAws = "minio_secret_key";
        System.setProperty("aws.accessKeyId",s3accessKeyAws);
        System.setProperty("aws.secretAccessKey",s3secretKeyAws);
        String connectionTimeOut = "600000";
        String s3endPointLoc = "http://127.0.0.1:9095";
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
                .set("fs.s3a.endpoint", s3endPointLoc)
                .set("fs.s3a.access.key", s3accessKeyAws)
                .set("fs.s3a.secret.key", s3secretKeyAws)
                .set("fs.s3a.connection.timeout", connectionTimeOut)
                .set("fs.s3a.path.style.access", "true")
                .set("spark.sql.debug.maxToStringFields", "100")
                .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .set("fs.s3a.connection.ssl.enabled", "true")
                .set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.my_catalog","org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.my_catalog.catalog-impl","org.apache.iceberg.jdbc.JdbcCatalog")
                .set("spark.sql.catalog.my_catalog.uri","jdbc:postgresql://127.0.0.1:5432/postgres")
                .set("spark.sql.catalog.my_catalog.jdbc.user","postgres")
                .set("spark.sql.catalog.my_catalog.jdbc.password","postgres")
                .set("spark.sql.catalog.my_catalog.warehouse","s3://iceberg")
                .set("spark.sql.catalog.my_catalog.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
                .set("spark.sql.catalog.my_catalog.s3.endpoint","http://127.0.0.1:9095")
                .set("spark.sql.catalogImplementation","in-memory")
               ;

        SparkSession spark = SparkSession.builder().appName("Example Iceberg Spark App").config(sparkConf).getOrCreate();
        /*
        spark.sql("CREATE TABLE my_catalog.my_table (\n" +
                "id bigint,\n" +
                "data string,\n" +
                "category string)\n" +
                "USING iceberg\n" +
                "LOCATION 's3://iceberg'\n" +
                "PARTITIONED BY (category)");
        */

        spark.sql("CREATE OR REPLACE TABLE my_catalog.trips (ts bigint, uuid string, rider string, driver string, fare float, city string) " +
                "USING iceberg PARTITIONED BY (city)");

        StructType structType = new StructType();
        structType = structType.add("ts", DataTypes.LongType, false);
        structType = structType.add("uuid", DataTypes.StringType, false);
        structType = structType.add("rider", DataTypes.StringType, false);
        structType = structType.add("driver", DataTypes.StringType, false);
        structType = structType.add("fare", DataTypes.DoubleType, false);
        structType = structType.add("city", DataTypes.StringType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create(1695159649087L,"334e26e9-8355-45cc-97c6-c31daf0df330","rider-A","driver-K",19.10,"san_francisco"));
        nums.add(RowFactory.create(1695516137016L,"e3cf430c-889d-4015-bc98-59bdce1e530c","rider-F","driver-P",34.15,"sao_paulo" ));
        nums.add(RowFactory.create(1695115999911L,"c8abbe79-8d89-47ea-b4ce-4d224bae5bfa","rider-J","driver-T",20.85,"chennai"));

        Dataset<Row> dataset = spark.createDataFrame(nums, structType);
        dataset.writeTo("my_catalog.trips").append();

    }
}
