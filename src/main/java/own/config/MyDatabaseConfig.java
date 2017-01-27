package own.config;

/**
 * @author Tushar Chokshi @ 1/17/17.
 */
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class MyDatabaseConfig {

    @Value("${couchbase.nodes}")
    private List<String> nodes = new ArrayList<>();

    @Value("${couchbase.bucket}")
    private String bucket;

    @Value("${couchbase.password}")
    private String password;

    public List<String> getNodes() {
        return nodes;
    }

    public String getBucket() {
        return bucket;
    }

    public String getPassword() {
        return password;
    }
}