package graphDB;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang.math.NumberUtils;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

/**
 * Created by ali on 19/02/17.
 */
public class Importer {


    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

        Class.forName("org.postgresql.Driver");
        String url = "jdbc:postgresql://localhost:5432/twitter";
        Connection conn = DriverManager.getConnection(url, "postgres", "fikhlonh");
        conn.setAutoCommit(false);

        ArrayList<String> notMatchedLines = new ArrayList<String>();


        HashMap<String, Map<String, Boolean>> tblNumeric = new HashMap<String, Map<String, Boolean>>();
        ArrayList<Record> records = new ArrayList<Record>();

        String pattern = "create \\(_(\\d+):`([^`]+)` (.*)\\)";
        Pattern r = Pattern.compile(pattern);
        for (String line : Files.readAllLines(Paths.get("entities.dump"))) {
            long id;
            String table, json;
            Matcher m = r.matcher(line);
            if (m.find()) {
                id = Long.parseLong(m.group(1));
                table = m.group(2);
                json = m.group(3);

            } else {
                notMatchedLines.add(line);
                continue;
            }
            Gson gson = new Gson();
            //System.out.println("JSON: " + json);
            JsonObject jsonRoot = new JsonParser().parse(json).getAsJsonObject();
            records.add(new Record(id, table, jsonRoot));
            jsonRoot.entrySet().stream().forEach(kv -> {
                if (!tblNumeric.containsKey(table))
                    tblNumeric.put(table, new HashMap<String, Boolean>());
                if (!tblNumeric.get(table).containsKey(kv.getKey()))
                    tblNumeric.get(table).put(kv.getKey(), NumberUtils.isNumber(kv.getValue().getAsString()));
                if (!tblNumeric.get(table).get(kv.getKey()) && !NumberUtils.isNumber(kv.getValue().getAsString()))
                    tblNumeric.get(table).put(kv.getKey(), false);
                //System.out.println("==>" + kv.getKey() + "=" + kv.getValue() + "  (" + kv.getKey().getClass().toString() + ")")
            });
            jsonRoot.getAsJsonObject().entrySet().stream().map(kv -> kv.getKey() + "=" + kv.getValue()).forEach(System.out::println);

        }


        for (String table : tblNumeric.keySet()) {
            Statement st = conn.createStatement();
            String q =  "DROP TABLE E_" + table + ";";
            st.executeUpdate(q);
            conn.commit();
            q =  "CREATE TABLE E_" + table + "(";
            q+= " _id BIGINT PRIMARY KEY NOT NULL";
            for (String c : tblNumeric.get(table).keySet())
                q+= ",  " + c.replace("`", "") + " " + (tblNumeric.get(table).get(c) ? "BIGINT" : "TEXT");
            q += ");";
            System.out.println(q);
            st.executeUpdate(q);

        }
        conn.commit();


        for (Record rec : records) {

            List<String> cols = rec.json.entrySet().stream().map(kv -> kv.getKey()).collect(Collectors.toList());
            String colString = cols.stream().map(k -> k.replace("`", "")).collect(Collectors.toList()).toString().substring(1);
            colString = " (_id, " + colString.substring(0, colString.length() - 1) + ")";
            colString = colString + " VALUES " + colString.replaceAll("[a-zA-Z_]+", "?");
            String query = "INSERT INTO E_" + rec.table + colString;
            System.out.println(query);
            PreparedStatement pstmt = conn.prepareStatement(query);
            // cast to the pg extension interface
            org.postgresql.PGStatement pgstmt = (org.postgresql.PGStatement) pstmt;
            // on the third execution start using server side statements
            pgstmt.setPrepareThreshold(3);

            int i = 1;
            pstmt.setBigDecimal(i++, BigDecimal.valueOf(rec.id));
            for(Map.Entry<String, JsonElement> e : rec.json.entrySet())
                if (tblNumeric.get(rec.table).get(e.getKey()))
                    pstmt.setBigDecimal(i++,e.getValue().getAsBigDecimal());
                else
                    pstmt.setString(i++,e.getValue().getAsString());

            pstmt.executeUpdate();
            System.out.println(pstmt.toString());
        }
        System.out.println("Committing...");
        conn.commit();
        System.out.println("DONE!");
        System.err.println("non-matches: ");
        for (String l : notMatchedLines)
            System.err.println(l);

    }
}
