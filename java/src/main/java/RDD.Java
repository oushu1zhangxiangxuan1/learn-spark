import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

SparkConf conf = new SparkConf().setAppName("RDD").setMaster("local[4]");
JavaSparkContext sc = new JavaSparkContext(conf);

List<Integer> data = Arrays.asList(1,2,3,4,5);
JavaRDD<Integer> distData = sc.parallelize(data);

System.println(distData.reduce((a, b) -> a + b));