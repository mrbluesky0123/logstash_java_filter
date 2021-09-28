package org.logstashplugins;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.logstashplugins.TelgrmParsingFilterTgw.telgrmMap;

public class TelgramInfoDao {

    private static Logger log = LogManager.getLogger();
    private Connection conn = null;

    public TelgramInfoDao() {
        this.getTelgrmInfo();
    }

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

    public void getTelgrmInfo() {

        List<TelgrmInfo> telgrmInfos = new ArrayList<>();
        PreparedStatement pstmt = null;

        this.connect();
        try {
            String sql =  "SELECT telgrm_no, field, field_size FROM telgrm_info ORDER BY telgrm_no, field_no";
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            String previousTelgrmNo = "";

            while (rs.next()) {
                String currentTelgrmNo = rs.getString("telgrm_no");

                if(!previousTelgrmNo.equals("#") && !currentTelgrmNo.equals(previousTelgrmNo)) {
                    telgrmMap.put(previousTelgrmNo, telgrmInfos);
                    telgrmInfos = new ArrayList<>();
                }

                TelgrmInfo telgrmInfo = new TelgrmInfo(rs.getString("field"), rs.getInt("field_size"));
                telgrmInfos.add(telgrmInfo);
                previousTelgrmNo = currentTelgrmNo;
            }

            // put last "telgrmInfos"
            telgrmMap.put(previousTelgrmNo, telgrmInfos);

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

    }

}
