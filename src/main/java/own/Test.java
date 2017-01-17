package own;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.concurrent.CountDownLatch;

/**
 * @author Tushar Chokshi @ 1/16/17.
 */
// http://docs.couchbase.com/developer/java-2.0/hello-couchbase.html
public class Test {
    public static void main(String[] args) throws Exception {

        // You do not need to pass in all nodes of the cluster, just a few seed nodes so that the client is able to establish initial contact.
        Cluster cluster = CouchbaseCluster.create("127.0.0.1");


        try {

            // The actual process of connecting to a bucket (that is, opening sockets and everything related) happens when you call the openBucket method:
            Bucket bucket = cluster.openBucket("travel-sample", "");

            ObjectMapper mapper = new ObjectMapper();

            Person person = createPerson();

            {
                // upserting document
                // if you try to insert already existing document, then it gives DocumentAlreadyExistsException.
                // upsert is like insert/update(replace)
                upsertPersonDocument(bucket, mapper, person);

                JsonDocument retrievePersonDocument = retrievePersonDocument(bucket, mapper, "Walter");
                Person upsertedPerson = convertJsonDocumentToPerson(mapper, retrievePersonDocument);
                System.out.println("Retrieved Upserted Person Document:" + upsertedPerson);// Person{firstName='Walter', lastName='White', age=27, job='chemistry teacher'}


                // Replacing document
                // You can think of replace as the opposite of insert—if the document does not already exist, the call fails.
                {
                    replacePersonDocument(bucket, mapper, upsertedPerson);

                    JsonDocument replacedPersonDocument = retrievePersonDocument(bucket, mapper, "Walter");
                    Person replacedPerson = convertJsonDocumentToPerson(mapper, replacedPersonDocument);
                    System.out.println("Retrieved Replaced Person Document:" + replacedPerson);// Person{firstName='Walter', lastName='White', age=27, job='chemistry teacher'}
                }
            }

            // Java 8 style, if you use async()
            final CountDownLatch latch = new CountDownLatch(1);
            bucket
                    .async()
                    .get("Walter") // returns Observable (like Optional>)
                    .flatMap(loaded -> {
                        loaded.content().put("age", 52);
                        return bucket.async().replace(loaded);
                    })
                    .subscribe(updated -> {
                        System.out.println("Updated: " + updated.id());
                        latch.countDown();
                    });

            latch.await();

        } finally {
            cluster.disconnect();
        }
    }

    protected static Person convertJsonDocumentToPerson(ObjectMapper mapper, JsonDocument retrievePersonDocument) throws java.io.IOException {
        return mapper.readValue(retrievePersonDocument.content().toString(), Person.class);
    }

    private static void upsertPersonDocument(Bucket bucket, ObjectMapper mapper, Person person) throws Exception {
        // To store the document, you can use the upsert method on the bucket. Because a document on the server has more properties than just the content, you need to give it at least a unique document ID (for example, walter).
        // If you replace upsert with insert and try to insert the same document twice (with the same ID), you see DocumentAlreadyExistsException

        //JsonObject person = createJsonObject(mapper);
        //JsonDocument walterDocument = JsonDocument.create(person.getString("firstName"), person);

        String personJsonString = createJsonString(mapper, person);
        RawJsonDocument walterDocument = RawJsonDocument.create(person.getFirstName(), personJsonString);

        // The Document is automatically converted into JSON and stored on the cluster. If the document (identified by its unique ID) already exists, it is replaced.
        RawJsonDocument upsertedPersonDocument = bucket.upsert(walterDocument);
        System.out.println("upserted Person Document: "+ upsertedPersonDocument);// upserted Person Document: RawJsonDocument{id='Walter', cas=91590245548032, expiry=0, content={"firstName":"Walter","lastName":"White","age":27,"job":"chemistry teacher"}, mutationToken=null}
    }

    private static JsonDocument retrievePersonDocument(Bucket bucket, ObjectMapper mapper, String key) throws Exception {
        JsonDocument walter = bucket.get(key);
        return walter;
    }

    private static RawJsonDocument replacePersonDocument(Bucket bucket, ObjectMapper mapper, Person person) throws Exception {
        person.setAge(50);

        RawJsonDocument walterDocument = RawJsonDocument.create(person.getFirstName(), createJsonString(mapper, person));
        return bucket.replace(walterDocument);
    }

    private static JsonObject createJsonObject(ObjectMapper mapper, Object object) throws Exception {
        String jsonInString = createJsonString(mapper, object);

        JsonObject jsonObject = mapper.readValue(jsonInString, JsonObject.class);

        return jsonObject;
    }

    private static Person createPerson() {
        return new Person("Walter", "White", 27, "chemistry teacher");
    }
    private static String createJsonString(ObjectMapper mapper, Object object) throws JsonProcessingException {
        return mapper.writeValueAsString(object);
    }
}
