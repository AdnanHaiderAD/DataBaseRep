package RUN;


import org.dejave.attica.server.Database;
import org.dejave.attica.storage.Tuple;
import org.dejave.attica.engine.operators.Sink;
import org.dejave.attica.engine.operators.EndOfStreamTuple;

public class RunMe {
    public static void main (String [] args) {
        try {
            // first argument should be the name of the properties file
            String propertiesFile = "/Users/Tanguero/Documents/MSC/Database_Systems/assignment1/attica-src/dist/attica.properties";
            // start up a new database instance
            Database db = new Database(propertiesFile);
            // run a select statement
            Sink sink = db.runStatement("select simple.key, simple.value from simple");

            // start iterating through the returned values
            boolean eos = false;
            while (! eos) {
                //retrieve the next tuple
                Tuple tuple = sink.getNext();
                System.out.println(tuple);
                //check whether this is the end of stream
                eos = (tuple instanceof EndOfStreamTuple);
            }
            // shut down the database
            db.shutdown();
        }
        catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);
        }
    } // main()
} // RunMe
