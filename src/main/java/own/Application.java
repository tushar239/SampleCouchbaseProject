package own;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

// https://github.com/couchbaselabs/beersample-java2

/*

What's needed?
--------------

- The beer-sample sample bucket

- The beer/brewery_beers view (built in beer-sample sample)
  If it is not built-in, then you can build it using below map function

    function (doc, meta) {
      if(doc.type == "beer" && doc.brewery_id) {
        emit(doc.brewery_id, meta.id);
      }
    }

- An additional view beer/by_name with the following map function (you should copy the beer designdoc to dev in order to edit it and add this view):

    function (doc, meta) {
       if (doc.type == "beer") {
         emit(doc.name, doc.brewery_id)
       }
     }

    If you want, you can set a reducer '_count' also to test your ViewQuery with group=true

Building and running
--------------------
- mvn clean package

- To run the application and expose the REST API on localhost:8080, run the following command:

java -jar target/beersample2-1.0-SNAPSHOT.jar
*/

@EnableAutoConfiguration
@ComponentScan
public class Application {
    public static void main(String... args) {
        SpringApplication.run(Application.class, args);
    }
}