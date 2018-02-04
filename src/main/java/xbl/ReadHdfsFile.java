package xbl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

public class ReadHdfsFile extends UDF {
  public String evaluate(String inputStr){
    if(inputStr == null) return null;
    Text result = new Text();

    HashSet<String> names = new HashSet<String>();
    try{
      //从集群HDFS上读国家列表文件
      System.out.println("aaa");
      FileSystem fs = FileSystem.get(new Configuration());

      FSDataInputStream in = fs.open(new Path("hdfs://192.168.50.110:8020/home/hive/udf/cz_ip.txt"));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;
      while((line = br.readLine()) != null){
        names.add(line);
        System.out.println(line);
      }
      br.close();
    }catch (IOException e){
      e.printStackTrace();
    }


    return "xxx";
  }
  public static void main(String[] args){

    ReadHdfsFile example = new ReadHdfsFile();
    System.out.println("area:" + example.evaluate("223.104.105.18"));
    // System.out.println("city :" + example.evaluate("1.0.32.11"));
  }

 /* public  static void main(String[] args){
    String inputStr = "测试中国日本";

    HashSet<String> names = new HashSet<String>();
    try{

      FileReader is = new FileReader("本地文件路径/country_list.txt");
      BufferedReader br = new BufferedReader(is);
      String line;
      while((line = br.readLine()) != null){
        names.add(line);
      }
      is.close();
    }catch (IOException e){
      e.printStackTrace();
    }


    System.out.println("xx");
  }*/

}
