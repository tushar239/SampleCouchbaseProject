package own.service;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.ParameterizedN1qlQuery;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.AsyncViewRow;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import own.config.MyDatabaseConfig;
import rx.Observable;
import rx.functions.Func2;

import javax.annotation.PreDestroy;
import java.util.List;

import static com.couchbase.client.java.query.Select.select;
import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.Expression.x;


/*
 http://docs.couchbase.com/developer/java-2.1/tutorial.html
 https://developer.couchbase.com/documentation/server/current/sdk/java/n1ql-queries-with-sdk.html

 This class shows 3 ways to retrieve documents from Couchbase
 1. Directly from the bucket using document id (bucket.get(id))
 2. Directly from the bucket using N1QlQuery
 3. Use ViewQuery to query a View. This will give you doc id, view key, view value. If you want, you can make spring to retrieve a document from a bucket eagerly or lazily along with View data. ViewQuery has a property includeDocs that makes it eager or lazy. If it is false (default), then unless you do viewRow.document(), it won't go to bucket to fetch a document based on doc id retrieved from a view.

 Bucket can be queried Synchronously or Asynchronously.
 Asynchronous approach uses RxJava(Reactive Pattern)
*/

@Service
public class MyCouchbaseService {
    private final MyDatabaseConfig myDatabaseConfig;

    private final Bucket bucket;
    private final Cluster cluster;

    @Autowired
    public MyCouchbaseService(final MyDatabaseConfig myDatabaseConfig) {
        this.myDatabaseConfig = myDatabaseConfig;

        //  creates a new Couchbase connection object and makes the initial connection to the cluster. In this example, we supply a list of IP addresses obtained from the Database configuration object, populated by Spring Boot with the contents of the application.yml file. You can supply a string, or several strings concatenated with commas so that it can fall back to another node should a connection to a single node fail.
        this.cluster = CouchbaseCluster.create(myDatabaseConfig.getNodes());

        // creates a connection to the bucket defined in the configuration. The Couchbase Java SDK provides both synchronous and asynchronous APIs that allow you to harness easily the power of asynchronous computation while maintaining the simplicity of synchronous operations. In this case, we are choosing to connect to both the cluster and the bucket synchronously as most of our application will be required to be synchronous, loading data before a web page can be generated. However, the asynchronous API is explained later on for use in creating view queries.
        this.bucket = cluster.openBucket(myDatabaseConfig.getBucket(), myDatabaseConfig.getPassword());
    }

    // The disconnect method is included even though it is not explicitly called in this example. Spring framework will invoke the method annotated with PreDestroy when destroying the context and shutting down the application.
    @PreDestroy
    public void preDestroy() {
        if (this.cluster != null) {
            this.cluster.disconnect();
        }
    }

    /**
     * READ the document from database for a given doc id
     * <p>
     * When data is stored in Couchbase as JSON, it will be converted by the Java SDK into a JsonDocument object. This allows you to use a very simple JSON library, built into the Couchbase SDK, to access, modify and re-save the data held in the document.
     * <p>
     * Another important aspect is error management. When the document doesn't exist, the SDK simply returns null. But should another error condition arise, a specific exception will be thrown (like a TimeOutException wrapped in a RuntimeException if the server couldn't respond in time). So it is important to ensure that your application can handle the errors that the SDK will pass up to it.
     */
    public JsonDocument read(String id) {
        return bucket.get(id);
    }

    /**
     * READ the document asynchronously from database.
     */
    public Observable<JsonDocument> asyncRead(String id) {
        return bucket.async().get(id);
    }

    /**
     * https://developer.couchbase.com/documentation/server/current/sdk/java/n1ql-queries-with-sdk.html
     *
     * READ the documents from Bucket using N1qlQuery.
     */
    public N1qlQueryResult readUsingN1QLQuery() {
        // To use normal query (non-parameterized), use N1qlQuery.simple
        // Using N1qlParams to set adhoc as false for using prepared statements
        // N1qlParams params = N1qlParams.build().adhoc(false);
        // N1qlQuery query = N1qlQuery.simple("select count(*) from `mybucket`", params);

        // To use Parameterized query, use N1qlQuery.parameterized
        Statement statement =
                select("name", "category", "abv")
                .from(i("beer-sample"))
                        .where(x("type").eq(x("$type"))
                                .and(x("abv").gt(x("$abv"))))
                        .limit(10);

        JsonObject placeholderValues = JsonObject.create().put("type", "beer").put("abv", 0);

        ParameterizedN1qlQuery parameterizedQuery = N1qlQuery.parameterized(statement, placeholderValues);

        N1qlQueryResult queryResult = bucket.query(parameterizedQuery);

        return queryResult;

    }


    /**
     * This method is querying a View 'by_name' and retrieves document ids from a view. All these information is stored in ViewQuery.
     *
     * View
     *     function (doc, meta) {
     *          if (doc.type == "beer") {
     *              emit(doc.name, doc.brewery_id)
     *          }
     *      }
     *  If you want, you can set a reducer '_count' also to test your ViewQuery with group=true
     *
     * Once ViewQuery is executed, you can retrieve view's key, value and doc id (basically all elements stored in the view)
     * It doesn't query a bucket at this time even thought you are executing a ViewQuery using bucket.query(viewQuery).
     * The only purpose to go to bucket is to fetch a document. By default, includeDocs=false in ViewQuery. It means that unless you try to fetch a row viewRow.document(), it won't go to bucket.
     * If you set includeDocs=true, then after executing a ViewQuery, it will go to bucket immediately to fetch the document.
     *
     * You can actually all features to filter the records from a view (like group, grouplevel, reduce, startKey/endKey etc.)
     */
    public ViewResult findAllBeers(Integer offset, Integer limit) {
        ViewQuery query = ViewQuery.from("beer" /*design document name*/, "by_name" /*view name*/);
        query.reduce(false); // if you have a reduce function in a view and if you don't want to use it then set reduce=false
        //query.includeDocs(false); // default
        //query.group(); // group or group-level can be used only if you have a reduce function in a view, otherwise you will get an error 'Invalid URL parameter 'group' or  'group_level' for non-reduce view.'

        if (limit != null && limit > 0) {
            query.limit(limit);
        }
        if (offset != null && offset > 0) {
            query.skip(offset);
        }
        ViewResult result = bucket.query(query);
        return result;
    }
    /**
     * Retrieves all the beers using a view query, returning the result asynchronously.
     *
     * Async operations use RxJava (Reactive Java library)'s Observable feature.
     */
    public Observable<AsyncViewResult> findAllBeersAsync() {
        ViewQuery allBeers = ViewQuery.from("beer", "by_name");
        return bucket.async().query(allBeers);
    }

    /**
     * From an async stream of all the beers and a search token, returns a stream
     * emitting a single JSON array. The array contains data for all matching beers,
     * each represented by three attributes: "id" (the beer's key), "name" (the beer's name)
     * and "detail" (the beers whole document content).
     */

    public Observable<JsonArray> searchBeer(Observable<AsyncViewRow> allBeers, final String token) {

        // Observable is just like Optional
        Observable<JsonDocument> jsonDocumentObservable = allBeers.flatMap(asyncViewRow -> asyncViewRow.document());

        Observable<JsonObject> jsonObjectObservable = jsonDocumentObservable.map(jd -> JsonObject.create().put("id", jd.id()).put("name", jd.content().getString("name")).put("detail", jd.content()));

        //reject beers that don't match the partial name
        Observable<JsonObject> filteredJsonObject = jsonObjectObservable.filter(jo -> {
            String name = jo.getString("name");
            return name != null && name.toLowerCase().contains(token.toLowerCase());
        });

        Observable<JsonArray> jsonArrayObservable = filteredJsonObject.collect(() -> JsonArray.empty(), (jsonArray, jsonObject) -> jsonArray.add(jsonObject));
        return jsonArrayObservable;

    }

    /**
     * Create a ViewQuery to retrieve all the beers for one single brewery.
     * The "\uefff" character (the largest UTF8 char) can be used to put an
     * upper limit to the brewery key retrieved by the view (which otherwise
     * would return all beers for all breweries).
     *
     * @param breweryId the brewery key for which to retrieve associated beers.
     */
    public static ViewQuery createQueryBeersForBrewery(String breweryId) {
        ViewQuery forBrewery = ViewQuery.from("beer", "brewery_beers");
        forBrewery.startKey(JsonArray.from(breweryId));
        //the trick here is that sorting is UTF8 based, uefff is the largest UTF8 char
        forBrewery.endKey(JsonArray.from(breweryId, "\uefff"));
        return forBrewery;
    }

    /**
     * Asynchronously query the database for all beers associated to a brewery.
     *
     * @param breweryId the brewery key for which to retrieve associated beers.
     * @see #createQueryBeersForBrewery(String)
     */
    public Observable<AsyncViewResult> findBeersForBreweryAsync(String breweryId) {
        return bucket.async().query(createQueryBeersForBrewery(breweryId));
    }

    /**
     * From a brewery document and a list of documents for its associated beers,
     * both asynchronously represented, prepare a stream of JSON documents concatenating
     * the data.
     * <p>
     * Each returned document is similar to the brewery document, but with a JSON array
     * of beer info under the "beers" attribute. Each beer info is a JSON object with an "id"
     * attribute (the key for the beer) and "beer" attribute (the original whole beer data).
     */
    public static Observable<JsonDocument> concatBeerInfoToBrewery(Observable<JsonDocument> brewery,
                                                                   Observable<List<JsonDocument>> beers) {
        return Observable.zip(brewery, beers,
                new Func2<JsonDocument, List<JsonDocument>, JsonDocument>() {
                    @Override
                    public JsonDocument call(JsonDocument breweryDoc, List<JsonDocument> beersDoc) {
                        JsonArray beers = JsonArray.create();
                        for (JsonDocument beerDoc : beersDoc) {
                            JsonObject beer = JsonObject.create()
                                    .put("id", beerDoc.id())
                                    .put("beer", beerDoc.content());
                            beers.add(beer);
                        }
                        breweryDoc.content().put("beers", beers);
                        return breweryDoc;
                    }
                });
    }

}
