package com.sene;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;


public class EtlBatchClass implements Job{
    static final String redshiftConnectionDEV = "jdbc:redshift://ddm-dev-dw-rs.cgtezcyc68ol.ap-northeast-2.redshift.amazonaws.com:45439/ldddmp";
    static final String redshiftConnectionPRD = "jdbc:redshift://ddm-prd-dw-rs.cgtezcyc68ol.ap-northeast-2.redshift.amazonaws.com:45439/ldddmp";
    static final String redshiftUserName = "ldfsdba";  // dev
    static final String redshiftUserPW = "Password1!"; // dev
    static final String mySqlConnectionDEV = "jdbc:mysql://ddm-dev-repodb-dev-rds.cazecky1swb8.ap-northeast-2.rds.amazonaws.com:3306/dpp?characterEncoding=UTF-8&serverTimezone=Asia/Seoul";
    static final String mySqlConnectionPRD = "jdbc:mysql://ddm-prd-repodb-dev-rds.cazecky1swb8.ap-northeast-2.rds.amazonaws.com:3306/dpp?characterEncoding=UTF-8&serverTimezone=Asia/Seoul";
    static final String mySqlUserName = "ldfsdba";
    static final String mySqlUserPW = "ldfsdbA123!#%";


    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        Connection redShiftConn = null;
        Statement redShiftStmt = null;
        Connection mySqlConn = null;
        Statement mySqlStmt = null;
        System.out.println(">>>>>>>>>>> ETL Quartz Start 시작 : " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")));

        try{
            Class.forName("com.amazon.redshift.jdbc.Driver");
            Driver mySqlDriver = (Driver) Class.forName("com.mysql.cj.jdbc.Driver").newInstance();

            System.out.println("Connecting to database...");
            Properties redShiftProps = new Properties();
            Properties mySqlProps = new Properties();

            //Uncomment the following line if using a keystore.
            //props.setProperty("ssl", "true");
            redShiftProps.setProperty("user", redshiftUserName);
            redShiftProps.setProperty("password", redshiftUserPW);
            redShiftConn = DriverManager.getConnection(redshiftConnectionDEV, redShiftProps);
            redShiftStmt = redShiftConn.createStatement();

            //mySql
            mySqlProps.setProperty("user", mySqlUserName);
            mySqlProps.setProperty("password", mySqlUserPW);
            mySqlConn = mySqlDriver.connect(mySqlConnectionDEV, mySqlProps);
            mySqlStmt = mySqlConn.createStatement();

            //Try a simple query.
            System.out.println("Listing system tables...");
            String sql;
            sql = "select * from ldddmp.ddmdm_test.tb_aly_ofr_prmtn_addtn_xclud_f limit 10;";
            ResultSet redShiftRs = redShiftStmt.executeQuery(sql);

            //Get the data from the result set.
            while(redShiftRs.next()){
                //Retrieve two columns.
                String feat_nm = redShiftRs.getString("cust360_feat_nm");
                String feat_val = redShiftRs.getString("cust360_feat_val");

                //Display values.
                System.out.print("feat_nm: " + feat_nm);
                System.out.println(", feat_val: " + feat_val);
            }

            // MySQL 쿼리 샘플
            System.out.println("MySQL 접속 시작");
            String mySqlSample = "select * from dpp.users limit 10;";
            ResultSet mySqlRs = mySqlStmt.executeQuery(mySqlSample);

            while (mySqlRs.next()){
                String login_id = mySqlRs.getString("login_id");
                String nickname = mySqlRs.getString("nickname");

                System.out.print("login_id: " + login_id);
                System.out.println(", nickname: " + nickname);
            }

            redShiftRs.close();
            redShiftStmt.close();
            redShiftConn.close();
            mySqlRs.close();
            mySqlStmt.close();
            mySqlConn.close();

        }catch (Exception e){
            e.printStackTrace();
        }finally{
            //Finally block to close resources.
            try{
                if(redShiftStmt!=null || mySqlStmt!=null)
                    redShiftStmt.close();
                    mySqlStmt.close();
            }catch(Exception ex){
            }// nothing we can do
            try{
                if(redShiftConn!=null || mySqlConn!=null)
                    redShiftConn.close();
                    mySqlConn.close();
            }catch(Exception ex){
                ex.printStackTrace();
            }
        }
        System.out.println("Finished connectivity test. System Exit");
        System.exit(0);
    }
}
