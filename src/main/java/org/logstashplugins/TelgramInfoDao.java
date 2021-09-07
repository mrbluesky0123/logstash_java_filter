package org.logstashplugins;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class TelgramInfoDao {

    private Connection conn = null;

    public void connect() {

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        try {
            conn = DriverManager.getConnection("jdbc:mysql://198.13.47.188:19762/elastic", "mrbluesky", "kang12!@");
        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

    }

    public List<TelgrmInfo> getTelgrmInfo(String telgrmNo) {

        List<TelgrmInfo> telgrmInfos = new ArrayList<>();
        PreparedStatement pstmt = null;

        this.connect();
        try {
            String sql =  "SELECT field, field_size FROM telgrm_info WHERE telgrm_no = ? ORDER BY field_no";
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1,telgrmNo);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {

                TelgrmInfo telgrmInfo = new TelgrmInfo(rs.getString("field"), rs.getInt("field_size"));
                telgrmInfos.add(telgrmInfo);

            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
                conn.close();
            } catch (SQLException e){
                e.printStackTrace();
            }
        }
        return telgrmInfos;
    }

}
