import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;

import java.net.UnknownHostException;

public class Riak {

    public static class Person {
        public String name;
        public String lastName;
    }
    private static RiakCluster setUpCluster() throws UnknownHostException {
        RiakNode node = new RiakNode.Builder()
                .withRemoteAddress("127.0.0.1")
                .withRemotePort(8087)
                .build();

        RiakCluster cluster = new RiakCluster.Builder(node)
                .build();

        cluster.start();

        return cluster;
    }

    public static void main( String[] args ) {
        try {

            Person person = new Person();
            person.name = "Adam";
            person.lastName = "Nowak";

            Namespace bucket = new Namespace("s17579");

            Location quoteObjectLocation = new Location(bucket, "id1");

            StoreValue storeOp = new StoreValue.Builder(person)
                    .withLocation(quoteObjectLocation)
                    .build();

            RiakCluster cluster = setUpCluster();
            RiakClient client = new RiakClient(cluster);

            client.execute(storeOp);
            System.out.println("Obiekt zapisany do bazy: " + person.name + " " + person.lastName);

            FetchValue fetchOp = new FetchValue.Builder(quoteObjectLocation)
                    .build();
            RiakObject fetchedObject = client.execute(fetchOp).getValue(RiakObject.class);
            System.out.println("Obiekt pobrany z bazy: " + fetchedObject.getValue());

            person.lastName = "Kowalski";
            StoreValue updateOp = new StoreValue.Builder(person)
                    .withLocation(quoteObjectLocation)
                    .build();
            client.execute(updateOp);

            FetchValue fetchAfterUpdate = new FetchValue.Builder(quoteObjectLocation)
                    .build();
            RiakObject fetchedObjectAfterUpdate = client.execute(fetchAfterUpdate).getValue(RiakObject.class);
            System.out.println("Obiekt pobrany z bazy po update: " + fetchedObjectAfterUpdate.getValue());

            DeleteValue deleteOp = new DeleteValue.Builder(quoteObjectLocation)
                    .build();
            client.execute(deleteOp);
            System.out.println("Usunieto poprawnie");

            fetchOp = new FetchValue.Builder(quoteObjectLocation)
                    .build();
            fetchedObject = client.execute(fetchOp).getValue(RiakObject.class);
            if(fetchedObject==null) {
                System.out.println("Brak szukanego obiektu");
            }

            cluster.shutdown();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}