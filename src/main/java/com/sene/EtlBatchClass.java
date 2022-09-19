package com.sene;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;


public class EtlBatchClass implements Job{
//    static final String redshiftDriver = "jdbc:redshift://ddm-dev-dw-rs.cgtezcyc68ol.ap-northeast-2.redshift.amazonaws.com:45439/ldddmp";
//    static final String redshiftDB = "ldddmp";
//    static final String redshiftUserName = "ldfsdba";
//    static final String redshiftUserPW = "Password1!";

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
//        Connection conn = null;
//        Statement stmt = null;
        System.out.println(">>>>>>>>>>> ETL Quartz Start 시작 : " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")));

//        try{
//            Class.forName("com.amazon.redshift.jdbc.Driver");
//
//            System.out.println("Connecting to database...");
//            Properties props = new Properties();
//
//            //Uncomment the following line if using a keystore.
//            //props.setProperty("ssl", "true");
//            props.setProperty("user", redshiftUserName);
//            props.setProperty("password", redshiftUserPW);
//            conn = DriverManager.getConnection(redshiftDriver, props);
//
//            //Try a simple query.
//            System.out.println("Listing system tables...");
//            stmt = conn.createStatement();
//            String sql;
//            sql = "select * from information_schema.tables;";
//            ResultSet rs = stmt.executeQuery(sql);
//
//            //Get the data from the result set.
//            while(rs.next()){
//                //Retrieve two columns.
//                String catalog = rs.getString("table_catalog");
//                String name = rs.getString("table_name");
//
//                //Display values.
//                System.out.print("Catalog: " + catalog);
//                System.out.println(", Name: " + name);
//            }
//            rs.close();
//            stmt.close();
//            conn.close();
//        }catch (Exception e){
//            e.printStackTrace();
//        }finally{
//            //Finally block to close resources.
//            try{
//                if(stmt!=null)
//                    stmt.close();
//            }catch(Exception ex){
//            }// nothing we can do
//            try{
//                if(conn!=null)
//                    conn.close();
//            }catch(Exception ex){
//                ex.printStackTrace();
//            }
//        }
        System.out.println("Finished connectivity test.");
    }
}
