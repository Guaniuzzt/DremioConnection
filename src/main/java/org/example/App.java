package org.example;


import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;


import javax.sql.DataSource;
import java.sql.*;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ){

        // For Dremio Cloud, the string, user & password would be as follows. Doc: https://docs.dremio.com/cloud/client-applications/jdbc/
        final String DB_URL = "jdbc:dremio:direct=sql.dremio.cloud:443;ssl=true;PROJECT_ID=a831f8eb-3e37-4d28-8d04-04c4f0a720af;";
        final String USER = "$token";
        final String PASS = "CeXySijHRIuFqXX/JXHZfgtH9vX2MlnA++Wmsv8aBcEIUm1EUfY54onEsVsnLg==";
        Properties props = new Properties();
        props.setProperty("user",USER);
        props.setProperty("password",PASS);

        //query(DB_URL, props);
        //update(DB_URL, props);
        //add(DB_URL, props);
        queryAfterAdd(DB_URL, props);
    }


    public static void query(String DB_URL, Properties props){
        try{
            System.out.println("Connecting to database...");
            Connection conn = DriverManager.getConnection(DB_URL,props);

            System.out.println("Creating statement...");
            Statement stmt = conn.createStatement();
            // ===============SELECT QUERY ====================
            String sql;
            sql = "SELECT * FROM \"my aws glue\".db.\"nyc_worker_coops\" " +
                    "WHERE Address1 = 'PO Box 8206'";
            System.out.println("Executing statement...");
            ResultSet rs = stmt.executeQuery(sql);

            System.out.print("Printing the result\n");
            System.out.print("-------------------\n");

            while (rs.next()) {
                System.out.print("City: " + rs.getString("City") + "\n");

            }
            System.out.print("-------------------\n");
            rs.close();
            stmt.close();
            conn.close();
        }catch(SQLException se){
            se.printStackTrace();
        }

    }

    public static void update(String DB_URL, Properties props){
        try {
            System.out.println("Connecting to database...");
            Connection conn = DriverManager.getConnection(DB_URL,props);

            System.out.println("Creating statement...");
            Statement stmt = conn.createStatement();
            // =================UPDATE================


            String sql = "UPDATE \"my aws glue\".db.\"nyc_worker_coops\" SET City = 'Jackson updated2' WHERE Address1 = 'PO Box 8206'";
            System.out.println("Executing UPDATE statement...");
            stmt.executeUpdate(sql);
            // --------------------
            System.out.print("-------------------\n");
            stmt.close();
            conn.close();
        } catch (SQLException se) {
            se.printStackTrace();
        }
    }

    public static void add(String DB_URL, Properties props){
        try{
            System.out.println("Connecting to database...");
            Connection conn = DriverManager.getConnection(DB_URL,props);

            System.out.println("Creating statement...");
            Statement stmt = conn.createStatement();
            //  ============= Add ================

            String sql = "INSERT INTO \"my aws glue\".db.\"nyc_worker_coops\" VALUES ('Georgezzt', 'Huaihan Street', NULL, 'Shanghai', NULL, 201100, NULL, NULL, 10, 12, 'Developer', NULL, 40, 10, 20, 30, NULL, NULL, NULL);";
            System.out.println("Executing ADD statement...");
            stmt.execute(sql);
            System.out.print("-------------------\n");
            stmt.close();
            conn.close();

        }catch(SQLException se){
            se.printStackTrace();
        }
    }


    public static void queryAfterAdd(String DB_URL, Properties props){
        try{
            System.out.println("Connecting to database...");
            Connection conn = DriverManager.getConnection(DB_URL,props);

            System.out.println("Creating statement...");
            Statement stmt = conn.createStatement();
            //  ============= Add ================

            //String sql = "INSERT INTO nyc_worker_coops VALUES ('Georgezzt', 'Huaihan Street', NULL, 'Shanghai', NULL, 201100, NULL, NULL, 10, 12, 'Developer', NULL, 40, 10, 20, 30, NULL, NULL, NULL);";
            String sql = "SELECT * FROM \"my aws glue\".db.\"nyc_worker_coops\" " +
                    "WHERE Address1 = 'Huaihan Street'";
            System.out.println("Executing ADD statement...");
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                System.out.print("City: " + rs.getString("City") + "\n");

            }
            System.out.print("-------------------\n");
            stmt.close();
            conn.close();

        }catch(SQLException se){
            se.printStackTrace();
        }
    }




}
