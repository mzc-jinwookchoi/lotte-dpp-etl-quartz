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
        Connection conn = null;
        Statement stmt = null;
        System.out.println(">>>>>>>>>>> ETL Quartz Start 시작 : " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")));

        try{
            Class.forName("com.amazon.redshift.jdbc.Driver");

            System.out.println("Connecting to database...");
            Properties props = new Properties();

            //Uncomment the following line if using a keystore.
            //props.setProperty("ssl", "true");
            props.setProperty("user", redshiftUserName);
            props.setProperty("password", redshiftUserPW);
            conn = DriverManager.getConnection(redshiftConnectionDEV, props);

            //Try a simple query.
            System.out.println("Listing system tables...");
            stmt = conn.createStatement();
            String sql;
            sql = "select * from ldddmp.ddmdm_test.tb_aly_ofr_prmtn_addtn_xclud_f limit 10;";
            ResultSet rs = stmt.executeQuery(sql);

            //Get the data from the result set.
            while(rs.next()){
                //Retrieve two columns.
                String feat_nm = rs.getString("cust360_feat_nm");
                String feat_val = rs.getString("cust360_feat_val");

                //Display values.
                System.out.print("feat_nm: " + feat_nm);
                System.out.println(", feat_val: " + feat_val);
            }

            // MySQL 쿼리 샘플
            System.out.println("MySQL 접속 시작");
            try(Connection mySqlConnection = DriverManager.getConnection(mySqlConnectionDEV, mySqlUserName, mySqlUserPW);
            Statement mySqlStmt = mySqlConnection.createStatement()){
                System.out.println("MySQL 접속 완료");
                String mySqlSampleQuery;
                mySqlSampleQuery = "SELECT * FROM dpp.users limit 10";
                ResultSet mySqlRs = mySqlStmt.executeQuery(mySqlSampleQuery);

                while (mySqlRs.next()){
                    String login_id = mySqlRs.getString("login_id");
                    String nickname = mySqlRs.getString("nickname");

                    System.out.print("login_id: " + login_id);
                    System.out.println(", nickname: " + nickname);
                }
                mySqlRs.close();
                mySqlStmt.close();
                mySqlConnection.close();
            }catch (SQLException e){
                e.printStackTrace();
            }catch (Exception e){
                e.printStackTrace();
            }
            rs.close();
            stmt.close();
            conn.close();

        }catch (Exception e){
            e.printStackTrace();
        }finally{
            //Finally block to close resources.
            try{
                if(stmt!=null)
                    stmt.close();
            }catch(Exception ex){
            }// nothing we can do
            try{
                if(conn!=null)
                    conn.close();
            }catch(Exception ex){
                ex.printStackTrace();
            }
        }
        System.out.println("Finished connectivity test.");
    }
}
