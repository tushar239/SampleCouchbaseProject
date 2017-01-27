package own.bucketqueriesexample;

import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.deps.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Tushar Chokshi @ 1/16/17.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Person {

    private String type = "person";

    @JsonProperty("firstName")
    private String firstName;
    @JsonProperty("lastName")
    private String lastName;
    @JsonProperty("age")
    private Integer age;
    @JsonProperty("job")
    private String job;

    @JsonCreator
    public Person(
            @JsonProperty("firstName") String firstName,
            @JsonProperty("lastName") String lastName,
            @JsonProperty("age") Integer age,
            @JsonProperty("job") String job) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
        this.job = job;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public void setJob(String job) {
        this.job = job;
    }


    public String getType() {
        return type;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public Integer getAge() {
        return age;
    }

    public String getJob() {
        return job;
    }

    @Override
    public String toString() {
        return "Person{" +
                "firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", age=" + age +
                ", job='" + job + '\'' +
                '}';
    }
}
