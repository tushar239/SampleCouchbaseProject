package own.controller;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.AsyncViewResult;
import com.couchbase.client.java.view.AsyncViewRow;
import com.couchbase.client.java.view.ViewQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import own.service.MyCouchbaseService;
import rx.Observable;
import rx.functions.Func1;

import java.util.List;

/**
 * @author Tushar Chokshi @ 1/17/17.
 */
@RestController
@RequestMapping(value = "/mybrewery", produces = MediaType.APPLICATION_JSON_VALUE)
public class MyBreweriesController {

    private static final Logger LOGGER =  LoggerFactory.getLogger(MyBreweriesController.class);

    private final MyCouchbaseService couchbaseService;

    @Autowired
    public MyBreweriesController(final MyCouchbaseService couchbaseService) {
        this.couchbaseService = couchbaseService;
    }

    @RequestMapping("/{id}")
    public ResponseEntity<String> getBrewery(@PathVariable String id) {

        ViewQuery forBrewery = MyCouchbaseService.createQueryBeersForBrewery(id);

        Observable<JsonDocument> brewery = couchbaseService.asyncRead(id);
        Observable<List<JsonDocument>> beers =
                couchbaseService.findBeersForBreweryAsync(id)
                        //extract rows from the result
                        .flatMap(new Func1<AsyncViewResult, Observable<AsyncViewRow>>() {
                            @Override
                            public Observable<AsyncViewRow> call(AsyncViewResult asyncViewResult) {
                                return asyncViewResult.rows();
                            }
                        })
                        //extract the actual document (pair of brewery id and beer id)
                        .flatMap(new Func1<AsyncViewRow, Observable<JsonDocument>>() {
                            @Override
                            public Observable<JsonDocument> call(AsyncViewRow asyncViewRow) {
                                return asyncViewRow.document();
                            }
                        })
                        .toList();

        //in the next observable we'll transform list of brewery-beer pairs into an array of beers
        //then we'll inject it into the associated brewery jsonObject
        Observable<JsonDocument> fullBeers = couchbaseService.concatBeerInfoToBrewery(brewery, beers)
                //take care of the case where no corresponding brewery info was found
                .singleOrDefault(JsonDocument.create("empty",
                        JsonObject.create().put("error", "brewery " + id + " not found")))
                //log errors and return a json describing the error if one arises
                .onErrorReturn(new Func1<Throwable, JsonDocument>() {
                    @Override
                    public JsonDocument call(Throwable throwable) {
                        LOGGER.warn("Couldn't get beers", throwable);
                        return JsonDocument.create("error",
                                JsonObject.create().put("error", throwable.getMessage()));
                    }
                });

        try {
            return new ResponseEntity<String>(fullBeers.toBlocking().single().content().toString(), HttpStatus.OK);
        } catch (Exception e) {
            LOGGER.error("Unable to get brewery " + id, e);
            return new ResponseEntity<String>(HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }


}