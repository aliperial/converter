package graphDB;

import com.google.gson.JsonObject;

/**
 * Created by ali on 20/02/17.
 */
public class Record {
    public long id;
    public String table;
    public JsonObject json;

    public Record(long id, String table, JsonObject json) {
        this.id = id;
        this.table = table;
        this.json = json;
    }
}
