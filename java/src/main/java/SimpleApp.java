/* SimpleApp.java*/
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SimpleApp{
    public static void main(String[] args){
        String logFile = "/usr/local/Cellar/spark/README.md";
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logFile).cache();

        long numAs = logData.filter((String s) -> s.contains("a")).count();
        long numBs = logData.filter((String s) -> s.contains("b")).count();
        
        System.out.println("Lines with a:"+numAs+", lines with b:"+numBs);

System.out.println("adfasfasssssssssssssssssfafafasfsafsafsssssssssssssssssssss");
System.out.println("adfasfasssssssssssssssssfafafasfsafsafsssssssssssssssssssss");
System.out.println("adfasfasssssssssssssssssfafafasfsafsafsssssssssssssssssssss");
System.out.println("adfasfasssssssssssssssssfafafasfsafsafsssssssssssssssssssss");


        spark.stop();
    }
}