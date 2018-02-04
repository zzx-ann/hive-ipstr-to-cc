package xbl;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Description(
        name="IpToCcByMysql",
        value="returns 'country_province_city', where ipstr is whatever you give it (string)",
        extended="select iptocity(ip) from t_uinfo;"
)

public class IpToCcByMysql  extends UDF {
    private final TreeMap<Long, String> ipcountry = new TreeMap<Long, String>();
    private final TreeMap<Long, String> ipprovince = new TreeMap<Long, String>();
    private final TreeMap<Long, String> ipcity = new TreeMap<Long, String>();
    private final TreeMap<Long, Long> ipMap = new TreeMap<Long, Long>();
    private final Map<String, String> provinceMap = new HashMap<String, String>();

    public String evaluate(String ipstr) {
        try {
            if(isEmpty()) {
                loadCfg();
            }

            if (!isIPv4Address(ipstr.toString())) {
                return "not ip";
            }

            String city = getCountryCode(ipstr.toString());

            return city;
        }catch(Exception e){
            e.printStackTrace();
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw, true));
            String strs = sw.toString();
            return "error info :" +e.getMessage()+"***"+e.fillInStackTrace().getMessage()+"***"+strs+"***";
        }
    }

    public void addIpRange(long startIp, long endIp, String country, String province, String city) {
        ipMap.put(startIp,endIp);
        ipcity.put(endIp, city);
        ipprovince.put(endIp, province);
        ipcountry.put(endIp,country);
    }

    public String getCountryCode(String ipstr) {
        long ip = ipToLong(ipstr);
        Map.Entry<Long, Long> entry = ipMap.floorEntry(ip);
        String recountry = null;
        String reprovince = null;
        String recity = null;
        if (entry != null && ip <= entry.getValue()) {
            if( ipcountry.get(entry.getValue()) == null || ipcountry.get(entry.getValue()).length() <= 0 ){
                recountry = "unknown";
            }else {
                recountry = ipcountry.get(entry.getValue());
            }
            if( ipprovince.get(entry.getValue()) == null || ipprovince.get(entry.getValue()).length() <= 0 ){
                reprovince = "unknown";
            }else {
                reprovince = ipprovince.get(entry.getValue());
            }
            if( ipcity.get(entry.getValue()) == null || ipcity.get(entry.getValue()).length() <= 0){
                recity = "unknown";
            }else {
                recity = ipcity.get(entry.getValue());
            }

            return recountry+"_"+reprovince+"_"+recity;
        } else {
            return "unknown_unknown_unknown";
        }
    }

    public boolean isEmpty() {
        return ipMap.isEmpty();
    }

    public static boolean isIPv4Address(String ipv4Addr) {
        String lower = "(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])"; // 0-255的数字
        String regex = lower + "(\\." + lower + "){3}";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(ipv4Addr);
        return matcher.matches();
    }

    public void loadCfg() throws SQLException {
        provinceMap.put("北京", "北京");
        provinceMap.put("天津", "天津");
        provinceMap.put("河北", "河北");
        provinceMap.put("山西", "山西");
        provinceMap.put("内蒙", "内蒙古");
        provinceMap.put("辽宁", "辽宁");
        provinceMap.put("吉林", "吉林");
        provinceMap.put("黑龙", "黑龙江");
        provinceMap.put("上海", "上海");
        provinceMap.put("江苏", "江苏");
        provinceMap.put("浙江", "浙江");
        provinceMap.put("安徽", "安徽");
        provinceMap.put("福建", "福建");
        provinceMap.put("江西", "江西");
        provinceMap.put("山东", "山东");
        provinceMap.put("河南", "河南");
        provinceMap.put("湖北", "湖北");
        provinceMap.put("湖南", "湖南");
        provinceMap.put("广东", "广东");
        provinceMap.put("广西", "广西");
        provinceMap.put("海南", "海南");
        provinceMap.put("重庆", "重庆");
        provinceMap.put("四川", "四川");
        provinceMap.put("贵州", "贵州");
        provinceMap.put("云南", "云南");
        provinceMap.put("西藏", "西藏");
        provinceMap.put("陕西", "陕西");
        provinceMap.put("甘肃", "甘肃");
        provinceMap.put("青海", "青海");
        provinceMap.put("宁夏", "宁夏");
        provinceMap.put("新疆", "新疆");
        provinceMap.put("台湾", "台湾");
        provinceMap.put("香港", "香港");
        provinceMap.put("澳门", "澳门");

        String driverName="com.mysql.jdbc.Driver";
        String url="jdbc:mysql://192.168.50.109:3306/test";
        String userName="hive";
        String password="hive";

        Connection conn = null;
        Statement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName(driverName);
            // System.out.println("连接数据库...");
            conn = DriverManager.getConnection(url,userName,password);
            //System.out.println(" 实例化Statement对...");
            stmt = conn.createStatement();
            String sql;
            //sql = "select FstartIP, FendIP, Fcountry, Fprovince, Fcity from t_cz_ipstr_copy where Fcity != '' limit 10;";
            sql = "select FstartIP, FendIP, Fcountry, Fprovince, Fcity from t_cz_ipstr_copy;";
            ResultSet rs = stmt.executeQuery(sql);

            Long startIP = 0L;
            Long endIP = 0L;
            String country = null;
            String province = null;
            String city = null;
            while(rs.next()){
                startIP = ipToLong(rs.getString("FstartIP"));
                endIP = ipToLong(rs.getString("FendIP"));
                country = rs.getString("Fcountry");
                province = rs.getString("Fprovince");
                if (!province.isEmpty() && province.length() >=2) {
                    String key = province.substring(0, 2);
                    province = provinceMap.get(key);
                }

                city = rs.getString("Fcity");
                //System.out.println(startIP + "\t" + endIP + "\t"  + province);
                addIpRange(startIP, endIP, country, province, city);
            }

            rs.close();
            stmt.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
            //return "JDBC fail";
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
           /* StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw, true));
            String strs = sw.toString();
            return " Class.forName fail" +e.getMessage()+"*****"+e.fillInStackTrace().getMessage()+"****"+strs;*/
        }finally{
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
    }

    private static long ipToLong(String ipv4Addr) {
        if (!isIPv4Address(ipv4Addr))
            return 0;
        String[] ipAddressInArray = ipv4Addr.split("\\.");
        long result = 0;
        for (int i = 0; i < ipAddressInArray.length; i++) {
            try
            {
                int power = 3 - i;
                int ip = Integer.parseInt(ipAddressInArray[i].trim());
                result += ip * Math.pow(256, power);
            }
            catch (Exception e)
            {
                result = 0;
            }
        }
        return result;
    }

    public static void main(String[] args){

        IpToCcByMysql example = new IpToCcByMysql();
        System.out.println("area:" + example.evaluate("223.104.105.18"));
        // System.out.println("city :" + example.evaluate("1.0.32.11"));
    }
}