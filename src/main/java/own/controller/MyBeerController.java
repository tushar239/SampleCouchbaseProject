package own.controller;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import own.service.MyCouchbaseService;

import java.util.Iterator;
import java.util.Optional;

/**
 * @author Tushar Chokshi @ 1/27/17.
 */
@RestController
@RequestMapping("/mybeer")
public class MyBeerController {

    private final MyCouchbaseService myCouchbaseService;

    @Autowired
    public MyBeerController(MyCouchbaseService myCouchbaseService) {
        this.myCouchbaseService = myCouchbaseService;
    }

    // http://localhost:8080/mybeer/21st_amendment_brewery_cafe
    @RequestMapping(method = RequestMethod.GET, value = "/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> getBeer(@PathVariable String id) {
        JsonDocument doc = myCouchbaseService.read(id);
        if (doc != null) {
            return new ResponseEntity<>(doc.content().toString(), HttpStatus.OK);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    // http://localhost:8080/mybeer/usingN1QlQuery
    @RequestMapping(value = "/usingN1QlQuery", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> listBeersUsingN1QLQuery() {
        N1qlQueryResult n1qlQueryResult = myCouchbaseService.readUsingN1QLQuery();

        if(!n1qlQueryResult.finalSuccess()) {
            //TODO maybe detect type of error and change error code accordingly
            return new ResponseEntity<>(n1qlQueryResult.toString(), HttpStatus.INTERNAL_SERVER_ERROR);
        }

        JsonArray result = JsonArray.create();

        Optional.ofNullable(n1qlQueryResult)
                .map(n1qlQueryRes -> n1qlQueryRes.allRows())
                .ifPresent(rows -> {
                    for (N1qlQueryRow row : rows) {
                        JsonObject value = row.value();
                        result.add(value);
                    }
                });

        return new ResponseEntity<>(result.toString(), HttpStatus.OK);

    }

    // http://localhost:8080/mybeer/usingViewQuery
    // http://localhost:8080/mybeer/usingViewQuery?offset=0&limit=10
    @RequestMapping(value = "/usingViewQuery", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> listBeers(@RequestParam(required = false) Integer offset,
                                            @RequestParam(required = false) Integer limit) {
        ViewResult result = myCouchbaseService.findAllBeers(offset, limit);
        if (!result.success()) {
            //TODO maybe detect type of error and change error code accordingly
            return new ResponseEntity<>(result.error().toString(), HttpStatus.INTERNAL_SERVER_ERROR);
        } else {
            JsonArray keys = JsonArray.create();
            Iterator<ViewRow> iter = result.rows();
            while (iter.hasNext()) {
                ViewRow row = iter.next();
                JsonObject beer = JsonObject.create();
                beer.put("id", row.id()); // doc id
                beer.put("name", row.key()); // view key
                beer.put("value", row.value().toString()); // value of a view row
                beer.put("document", row.document().toString());

                keys.add(beer);
            }
            return new ResponseEntity<>(keys.toString(), HttpStatus.OK);
        }
    }


    // http://localhost:8080/mybeer/search/21st_amendment
    // This code is based on RxJava (Reactive Java)
    @RequestMapping(method = RequestMethod.GET, value = "/search/{token}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> searchBeer(@PathVariable final String token) {

        //we'll get all beers asynchronously and compose on the stream to extract those that match
        AsyncViewResult viewResult = myCouchbaseService.findAllBeersAsync().toBlocking().single();

        if (viewResult.success()) {
            return myCouchbaseService.searchBeer(viewResult.rows(), token)
                    //transform the array into a ResponseEntity with correct status
                    .map(objects -> new ResponseEntity<>(objects.toString(), HttpStatus.OK))
                    //in case of errors during this processing, return a ERROR 500 response with detail
                    .onErrorReturn(throwable -> new ResponseEntity<String>("Error while parsing results - " + throwable,
                            HttpStatus.INTERNAL_SERVER_ERROR))
                    //block and send back the response
                    .toBlocking().single();
        } else {
            return new ResponseEntity<>("Error while searching - " + viewResult.error(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


}
