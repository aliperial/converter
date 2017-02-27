package ocean;

import com.google.gson.*;
import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by ali on 27/02/17.
 */
public class fixer {
    public static final String ARRAY_FIELD_NAME = "products"/*"parameter"*/;
    public static final String TABLE_NAME = "oceandata"/*"stations"*/;
    public static final String TARGET_TABLE_NAME = TABLE_NAME + "_" + ARRAY_FIELD_NAME;
    public static void main(String[] args) throws InterruptedException {
        Connection c = null;
        Statement stmt = null;
        try {
            Gson gson = new Gson();
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://146.169.46.53:5432/ocean",
                            "postgres", "thomasandme");
            System.out.println("Opened database successfully");

            stmt = c.createStatement();
            PreparedStatement ps = c.prepareStatement("DROP TABLE IF EXISTS " + TARGET_TABLE_NAME + ";");
            ps.executeUpdate();

            ps = c.prepareStatement("CREATE TABLE IF NOT EXISTS " + TARGET_TABLE_NAME + "(_id varchar(4000));");
            ps.executeUpdate();

            Set<String> seenKeys = new HashSet<String>();
            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery( "SELECT _id, " + ARRAY_FIELD_NAME + " FROM " + TABLE_NAME + ";" );
            while ( rs.next() ) {
                String  id = rs.getString("_id");
                String  arrayStr = rs.getString(ARRAY_FIELD_NAME);

                //handle nulls
                if(arrayStr == null || arrayStr.trim().length() == 0)
                    continue;

                //handle non-arrays
                if(arrayStr.trim().startsWith("{"))
                    arrayStr = "[" + arrayStr + "]";

                arrayStr = arrayStr.replace("\"t\": {\"$date", "\"t_date").replace("\"}, \"", "\", \"");

                Type listType = new TypeToken<List<String>>() {}.getType();
                //System.out.println(arrayStr);

                JsonArray arrayJson = new JsonParser().parse(arrayStr).getAsJsonArray();
                //System.out.println( "ID = " + id );
                //System.out.println( ARRAY_FIELD_NAME +  "= " + arrayStr );

                for (Iterator<JsonElement> singleJsons = arrayJson.iterator(); singleJsons.hasNext();){
                    JsonObject singleJson = singleJsons.next().getAsJsonObject();
                    System.out.println(singleJson.toString());
                    for(Map.Entry<String,JsonElement> e: singleJson.entrySet()){
                        if(!seenKeys.contains(e.getKey())){
                            System.out.println(e.getValue().toString());
                            String val = e.getValue().getAsString();
                            ps = c.prepareStatement("ALTER TABLE " + TARGET_TABLE_NAME + " add column \"" + e.getKey() + "\" varchar(4000);");
                            ps.executeUpdate();
                            seenKeys.add(e.getKey());
                        }
                    }

                    String part1 = "INSERT INTO " + TARGET_TABLE_NAME + "(_id";
                    String part2 = "VALUES (?";
                    List<String> valuesList = new ArrayList<String>();
                    for(Map.Entry<String,JsonElement> e: singleJson.entrySet()){
                        part1 += ",\"" + e.getKey() + "\"";
                        part2 += ",?";
                        valuesList.add(e.getValue().getAsString());
                    }

                    String query = part1 + ")" + part2 + ")";
                    //System.out.println(query);
                    ps = c.prepareStatement(query);
                    ps.setString(1,id);
                    int i = 2;
                    for(String v : valuesList) {
                        ps.setString(i++, v);
                    }
                    //System.out.println(ps.toString());
                    ps.executeUpdate();
                }
            }
            rs.close();
            stmt.close();
            c.close();
        } catch ( Exception e ) {
            TimeUnit.MILLISECONDS.sleep(300);
            e.printStackTrace();
            System.exit(0);
        }
    }

}
