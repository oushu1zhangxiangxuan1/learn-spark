
/* SimpleApp.java*/
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.List;

// import java.util.function.Function;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.util.Collections;

public class SimpleApp {
    // private

    public void RDD() {
        SparkConf conf = new SparkConf().setAppName("RDD").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data);

        System.out.println(distData.reduce((a, b) -> a + b));
        sc.close();
    }

    public void SimpleAppTest() {
        try {
            String logFile = "/usr/local/Cellar/spark/README.md";
            SparkSession spark = SparkSession.builder().appName("What a fuck Application").getOrCreate();
            Dataset<String> logData = spark.read().textFile(logFile).cache();
    
            long numAs = logData.filter((String s) -> s.contains("a")).count();
            long numBs = logData.filter((String s) -> s.contains("b")).count();
    
            System.out.println("Lines with a:" + numAs + ", lines with b:" + numBs);
            int i= 100;
            while (i>0){
                System.out.println("adfasfasssssssssssssssssfafafasfsafsafsssssssssssssssssssss");
                // wait(10);
                Thread.currentThread().sleep(10000);
                i--;
            }
            System.out.println("adfasfasssssssssssssssssfafafasfsafsafsssssssssssssssssssss");
            System.out.println("adfasfasssssssssssssssssfafafasfsafsafsssssssssssssssssssss");
            System.out.println("adfasfasssssssssssssssssfafafasfsafsafsssssssssssssssssssss");
    
            spark.stop();
            
        } catch (Exception e) {
            //TODO: handle exception
            System.out.println(e);
        }
    }

    public void ExtData() {
        SparkConf conf = new SparkConf().setAppName("ExtData").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> distFile = sc.textFile("/usr/local/Cellar/spark/README.md");

        // Dataset<String> ds = sc.textFile("/usr/local/Cellar/spark/README.md");

        // JavaRDD<String> ds = distFile.dar;
        // Integer count = sc.count();

        // DataSet<String> ds = distFile.map(s -> s.length());
        JavaRDD<Integer> linLengths = distFile.map(s -> s.length());
        int totalLength = linLengths.reduce((a, b) -> a + b);

        System.out.println(totalLength);

        sc.close();
    }

    public void Lambda() {
        SparkConf conf = new SparkConf().setAppName("Lambda").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> distFile = sc.textFile("/usr/local/Cellar/spark/README.md");

        JavaRDD<Integer> lineLengths = distFile.map(new Function<String, Integer>() {
            /**
            *
            */
            private static final long serialVersionUID = -6328742248300225956L;

            public Integer call(String s) {
                return s.length();
            }
        });

        int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
            /**
            *
            */
            private static final long serialVersionUID = 4732512393234366811L;

            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });

        System.out.println(totalLength);
        sc.close();
    }

    public void Unwieldy() {
        class GetLength implements Function<String, Integer> {
            /**
             *
             */
            private static final long serialVersionUID = -455086554696535783L;

            public Integer call(String s) {
                return s.length();
            }
        }

        class Sum implements Function2<Integer, Integer, Integer> {
            /**
             *
             */
            private static final long serialVersionUID = -6219261562373180379L;

            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        }

        SparkConf conf = new SparkConf().setAppName("Unwieldy").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/usr/local/Cellar/spark/README.md");

        JavaRDD<Integer> lineLengths = lines.map(new GetLength());
        int totalLength = lineLengths.reduce(new Sum());

        System.out.println("Unwieldy totallength:" + totalLength);
        sc.close();
    }

    public void KV() {

        SparkConf conf = new SparkConf().setAppName("Unwieldy").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/usr/local/Cellar/spark/README.md");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        System.out.println(counts.collect());
        sc.close();
    }

    public void Broadcast() {
        SparkConf conf = new SparkConf().setAppName("Unwieldy").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        Broadcast<int[]> broadcastVar = sc.broadcast(new int[] { 1, 2, 3 });

        System.out.println(broadcastVar.value().clone().toString());
        sc.close();
    }

    public void Accum() {
        SparkConf conf = new SparkConf().setAppName("Unwieldy").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        LongAccumulator accum = sc.sc().longAccumulator();
        sc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));
        System.out.println(accum.value());
        sc.close();
    }

    public void DatasetOp1() {
        SparkSession spark = SparkSession.builder().appName("DatasetOp1").getOrCreate();
        Dataset<Row> df = spark.read()
                .json("/Users/johnsaxon/go/src/github.com/oushu-io/spark/examples/src/main/resources/people.json");

        df.show();

        df.printSchema();

        df.select("name").show();

        df.select(col("name"), col("age").plus(1)).show();

        df.filter(col("age").gt(21)).show();

        df.groupBy("age").count().show();

        spark.stop();
    }

    public void SqlQuery1() {
        SparkSession spark = SparkSession.builder().appName("DatasetOp1").getOrCreate();
        Dataset<Row> df = spark.read()
                .json("/Users/johnsaxon/go/src/github.com/oushu-io/spark/examples/src/main/resources/people.json");

        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();

        // Global temporary view
        // Register
        try {
            df.createGlobalTempView("people");
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println(e.toString());
        }
        spark.sql("SELECT * from global_temp.people").show();
        spark.newSession().sql("SELECT * from global_temp.people").show();

        spark.stop();
    }

    // public void CreateDataset() {
    //     Person person = new Person();
    //     person.setName("Andy");
    //     person.setAge(32);

    //     SparkSession spark = SparkSession.builder().appName("DatasetOp1").getOrCreate();
    //     // Dataset<Row> df = spark.read()
    //     // .json("/Users/johnsaxon/go/src/github.com/oushu-io/spark/examples/src/main/resources/people.json");

    //     Encoder<Integer> integerEncoder = Encoders.INT();
    //     Dataset<Integer> primitiveDS = spark.CreateDataset(Collections.singletonList(person), personEncoder);
    // }

    public static void main(String[] args) {

        SimpleApp app = new SimpleApp();

        app.SimpleAppTest();
        // app.RDD();
        // app.ExtData();
        // app.Lambda();
        // app.Unwieldy();
        // app.KV();
        // app.Broadcast();
        // app.Accum();
        // app.DatasetOp1();
        // app.SqlQuery1();
    }
    // spark://localhost:7077
}