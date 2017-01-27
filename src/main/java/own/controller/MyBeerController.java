package own.controller;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
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

    // http://localhost:8080/mybeer
    // http://localhost:8080/mybeer?offset=0&limit=10
    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
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
                beer.put("name", row.key());
                beer.put("id", row.id());
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
