package own;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;

/**
 * @author Tushar Chokshi @ 1/16/17.
 */
public class MyDatabase {

    private String hostname = "127.0.0.1";

    private String travelSampleBucket = "travel-sample";

    private String password;

    Cluster cluster() {
        return CouchbaseCluster.create(hostname);
    }

    public Bucket travelSampleBucket() {
        return cluster().openBucket(travelSampleBucket, password);
    }

}
