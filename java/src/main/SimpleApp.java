/* SimpleApp.java*/
import org.apache.spark.sql.SparkSession

public class SimpleApp{
    public static void main(String[] args){
        String logFile = "/usr/local/Cellar/spark/README.md";
        SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
        Dataset<String> logData = spark.read.textFile(logFile).cache()

        long numAs = logData.filter(s -> s.contains("a")).count();
        long numBs = logData.filter(s->s.contains("b")).count();
        
        System.out.println("Lines with a:"+numAs+", lines with b:"+numBs);

System.out.println("adfasfasssssssssssssssssfafafasfsafsafsssssssssssssssssssss");
System.out.println("adfasfasssssssssssssssssfafafasfsafsafsssssssssssssssssssss");
System.out.println("adfasfasssssssssssssssssfafafasfsafsafsssssssssssssssssssss");
System.out.println("adfasfasssssssssssssssssfafafasfsafsafsssssssssssssssssssss");


        spark.stop();
    }
}