import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by ali on 20/02/17.
 */
public class DumpFix {

    public static void main(String[] args) throws IOException {

        List<String> a = Files.readAllLines(Paths.get("twitter.dump"));
        System.out.println(a.size());
        int i=0;
        while(true) {
            while (a.get(i).startsWith("create (") && ! a.get(i+1).startsWith("create ")) {
                System.out.println(a.get(i));
                a.set(i,a.get(i) + " " + a.get(i+1));
                a.remove(i + 1);
            }
            if(i++ > a.size()-2) break;
        }

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get("twitter_new.dump"))) {
            for (String s : a) {
                if(s.trim().equals("begin") || s.trim().equals("commit") || s.trim().equals(";")) continue;
                //writer.write("CYPHER 2.3\n");
                writer.write(s);
                //writer.write(";\n");
                writer.write("\n");
            }
        }

        System.out.println(a.size());


    }

}
