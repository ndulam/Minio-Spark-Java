package org.cloud.data;
//import org.apache.hudi.common.model.HoodieTableType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkAppMain {
    public static <Row> void main(String[] args) {
        System.out.println("Hello World");
        System.setProperty("hadoop.home.dir", "D:\\sparksetup\\hadoop");
        String s3accessKeyAws = "minio_access_key";
        String s3secretKeyAws = "minio_secret_key";
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
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.sql.warehouse.dir", "file:///C:/tmp/spark_shell/spark_warehouse")
                .set("spark.sql.hive.convertMetastoreParquet", "false")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                ;
        SparkSession spark = SparkSession.builder().appName("Example Spark App").config(sparkConf).getOrCreate();

        Dataset ds = spark.read().parquet("s3a://sales/sales_summary_updated.parquet");

        ds.write().mode(SaveMode.Overwrite).parquet("s3a://hudidata/test");

        System.out.print(ds.count());

    }
}
