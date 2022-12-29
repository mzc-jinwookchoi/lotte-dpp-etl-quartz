package com.sene;


import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;


public class EtlBatchClass{
    // Redshift, MySQL Connect Info
    static final String redshiftConnectionDEV = "jdbc:redshift://ddm-dev-dw-rs.cgtezcyc68ol.ap-northeast-2.redshift.amazonaws.com:45439/ldddmp";
    static final String redshiftConnectionPRD = "jdbc:redshift://ddm-prd-dw-rs.cgtezcyc68ol.ap-northeast-2.redshift.amazonaws.com:45439/ldddmp";
    static final String redshiftUserName = "ldfsdba";  // dev
    static final String redshiftUserPW = "Password1!"; // dev
    static final String mySqlConnectionDEV = "jdbc:mysql://ddm-dev-repodb-dev-rds.cazecky1swb8.ap-northeast-2.rds.amazonaws.com:3306/dpp?characterEncoding=UTF-8&serverTimezone=Asia/Seoul";
    static final String mySqlConnectionPRD = "jdbc:mysql://ddm-prd-repodb-dev-rds.cazecky1swb8.ap-northeast-2.rds.amazonaws.com:3306/dpp?characterEncoding=UTF-8&serverTimezone=Asia/Seoul";
    static final String mySqlUserName = "ldfsdba";
    static final String mySqlUserPW = "ldfsdbA123!#%";


    public void EtlProcess() {
        Connection redShiftConn = null;
        Statement redShiftStmt = null;
        Connection mySqlConn = null;
        Statement mySqlStmt = null;
        System.out.println(">>>>>>>>>>> MySQL -> Redshift ETL Program Start : " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")));

        try{
            Class.forName("com.amazon.redshift.jdbc.Driver");
            Driver mySqlDriver = (Driver) Class.forName("com.mysql.cj.jdbc.Driver").newInstance();

            System.out.println("Connecting to database...");
            Properties redShiftProps = new Properties();
            Properties mySqlProps = new Properties();

            //Redshift Connect
            redShiftProps.setProperty("user", redshiftUserName);
            redShiftProps.setProperty("password", redshiftUserPW);
            redShiftConn = DriverManager.getConnection(redshiftConnectionDEV, redShiftProps);
            redShiftStmt = redShiftConn.createStatement();

            //MySQL Connect
            mySqlProps.setProperty("user", mySqlUserName);
            mySqlProps.setProperty("password", mySqlUserPW);
            mySqlConn = mySqlDriver.connect(mySqlConnectionDEV, mySqlProps);
            mySqlStmt = mySqlConn.createStatement();

            //Query Migration Start
            System.out.println("Query Migration Start...");

            String targetSchema = "ldddmp.ddmdm_test";                                 // Target 스키마 설정
            String sourceSchema = "dpp_etl";                                           // Source 스키마 설정
            String targetTable = "tb_aly_ofr_prmtn_addtn_xclud_f";                      // Source, Target 테이블 설정
            String selectTargetSql = "SELECT * FROM " + targetSchema + "." + targetTable;     // ResultSetMetaData 를 추출 하기 위한 targetTable Select
            String selectSourceSql = "SELECT * FROM " + sourceSchema + "." + targetTable;     // ResultSetMetaData 를 추출 하기 위한 sourceTable Select
            String truncateTargetSql = "TRUNCATE " + targetSchema + "." + targetTable;        // Insert 이전 Truncate

            ResultSet redShiftRs = redShiftStmt.executeQuery(selectTargetSql);          // Redshift ResultSet 선언
            ResultSetMetaData redShiftRsMetaData = redShiftRs.getMetaData();            // Redshift ResultSetMetaData 선언
            ResultSet mySqlRs = mySqlStmt.executeQuery(selectSourceSql);                // MySQL ResultSet 선언
            ResultSetMetaData mySqlRsMetaData = mySqlRs.getMetaData();                  // MySQL ResultSetMetaData 선언

            StringBuffer stringBuffer = new StringBuffer();                             // insertMigrateSql 조합을 위한 StringBuffer 선언

            stringBuffer.append("INSERT INTO ");
            stringBuffer.append(targetSchema);
            stringBuffer.append(".");
            stringBuffer.append(targetTable);
            stringBuffer.append(" (");

            int columnCnt = mySqlRsMetaData.getColumnCount();                           // MySQL 컬럼 카운트
            String[] columnNames = new String[columnCnt];                               // MySQL 컬럼명 배열 선언

            for(int n=1; n<=columnCnt; n++){                                            // MySQL 컬럼명 추출 후 append
                columnNames[n-1] = mySqlRsMetaData.getColumnName(n);
//                System.out.println("columnNames : "+columnNames[n-1]);
                stringBuffer.append(columnNames[n-1]);
                if(n<columnCnt){
                    stringBuffer.append(", ");
                }else if(n==columnCnt){
                    stringBuffer.append(") VALUES ");
                }
            }

            String insertMigrateSql = stringBuffer.toString();
            System.out.println("insertMigrateSql : "+insertMigrateSql);
            int RsCnt = 0;
            while (mySqlRs.next()){
                if(RsCnt != 0){
                    stringBuffer.append(", ");
                }
                stringBuffer.append("(");
                for(int i=1; i<=columnCnt; i++){
                    int columnType = mySqlRsMetaData.getColumnType(i);  // 컬럼 타입 추출

                    if(columnType==Types.TIME){                         // 컬럼 타입 구분
                        stringBuffer.append("'");
                        stringBuffer.append(mySqlRs.getTime(i));
                        stringBuffer.append("'");
                    }else if(columnType==Types.TIMESTAMP){
                        stringBuffer.append("'");
                        stringBuffer.append(mySqlRs.getTimestamp(i));
                        stringBuffer.append("'");
                    }else if(columnType==Types.DATE){
                        stringBuffer.append("'");
                        stringBuffer.append(mySqlRs.getDate(i));
                        stringBuffer.append("'");
                    }else if(columnType==Types.INTEGER){
                        stringBuffer.append(mySqlRs.getInt(i));
                    }else {
                        stringBuffer.append("'");
                        stringBuffer.append(mySqlRs.getString(i));
                        stringBuffer.append("'");
                    }

                    if(i<columnCnt){
                        stringBuffer.append(", ");
                    }else if(i==columnCnt){
                        stringBuffer.append(")");
                    }
                }
                RsCnt++;
            }

            String insertMigrateSql2 = stringBuffer.toString();
            System.out.println("insertMigrateSql2 : "+insertMigrateSql2);
            System.out.println("columnCnt : "+columnCnt);
            System.out.println("RsCnt : "+RsCnt);

            System.out.println(">>>>>>>>>>> TargetTable TRUNCATE 시도");
            redShiftStmt.executeUpdate(truncateTargetSql);
            System.out.println(">>>>>>>>>>> TargetTable TRUNCATE 완료");
            System.out.println(">>>>>>>>>>> TargetTable INSERT 시도");
            redShiftStmt.executeUpdate(insertMigrateSql2);
            System.out.println(">>>>>>>>>>> TargetTable INSERT 완료");


            // rs, stmt, conn Close
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
        System.out.println(">>>>>>>>>>> MySQL -> Redshift ETL Program End : " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")));
        System.out.println(">>>>>>>>>>> ETL Program Exit : " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")));
        System.exit(0);
    }
}
