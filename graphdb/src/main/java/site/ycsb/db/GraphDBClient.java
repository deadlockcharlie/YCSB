package site.ycsb.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.db.DatabaseDrivers.DatabaseClient;
import site.ycsb.db.DatabaseDrivers.DatabaseClientFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class GraphDBClient extends DB {
  private static HttpClient httpClient;
  private static Properties props = new Properties();
  private final Logger log = LoggerFactory.getLogger(getClass());
  private DatabaseClient dbDriver;
  @Override
  public void init() throws DBException {
    System.out.println("Starting GraphDB client");
    props = getProperties();
    System.out.println(props);
    if (props.getProperty("HOSTURI") == null) {
      System.out.println("GraphDB HOSTURI must be provided");
      throw new DBException("GraphDB HOSTURI must be provided");
    }
    System.out.println("GraphDB HOSTURI: " + props.getProperty("HOSTURI"));

    if(props.getProperty("DBTYPE")==null){
      System.out.println("DBTYPE must be provided");
      throw new DBException("DBTYPE must be provided");
    }
    System.out.println("DBTYPE: "+props.getProperty("DBTYPE"));

    if(props.getProperty("DBURI")==null){
      System.out.println("DBURI must be provided");
      throw new DBException("DBURI must be provided");
    }
    System.out.println("DBURI: "+props.getProperty("DBURI"));

    dbDriver = DatabaseClientFactory.getDatabaseClient(props.getProperty("DBTYPE"));
    dbDriver.connect(props.getProperty("DBTYPE"), props.getProperty("DBURI"));



    httpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .followRedirects(HttpClient.Redirect.NORMAL)
        .connectTimeout(Duration.ofSeconds(5))
        .build();
  }

  @Override
  public Status addVertex(String label, String id, Map<String, ByteIterator> properties) {
    try{
    String query = "CREATE (n: YCSBVertex {id:'"+ id+"'";
    for ( Map.Entry<String, ByteIterator> entry : properties.entrySet() ) {
      String key = entry.getKey();
      String value = entry.getValue().toString();

      query += ", " + key + ": '" + value+"'";
    }
    query += "})";


    String reqBody = "{ \"query\": \""+ query+"\", \"params\": null }";
//    System.out.println("Request Body: " + reqBody);
    String targetString = props.getProperty("HOSTURI") + "/write";

    URI target = new URI(targetString);
    HttpRequest request = HttpRequest.newBuilder()
        .uri(target)
        .POST(HttpRequest.BodyPublishers.ofString(reqBody))
        .header("Content-Type", "application/json")
        .build();
    HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
//      System.out.println("Response status code: " + response);
    return Status.OK;
  } catch (Exception e) {
    return Status.ERROR;
  }
  }

  @Override
  public Status addEdge(String label, String id, String from, String to, Map<String, ByteIterator> properties) {
    try{
      String query = "MATCH (a {id: '"+ from +"'}), (b {id: '"+ to +"'}) CREATE (a)-[r: YCSBEdge {id:'"+ id+"'";
      for ( Map.Entry<String, ByteIterator> entry : properties.entrySet() ) {
        String key = entry.getKey();
        String value = entry.getValue().toString();

        query += ", " + key + ": '" + value+"'";
      }
      query += "}]->(b)";

      String reqBody = "{ \"query\": \""+ query+"\", \"params\": null }";
      System.out.println("Request Body: " + reqBody);
      String targetString = props.getProperty("HOSTURI") + "/write";

      URI target = new URI(targetString);
      HttpRequest request = HttpRequest.newBuilder()
          .uri(target)
          .POST(HttpRequest.BodyPublishers.ofString(reqBody))
          .header("Content-Type", "application/json")
          .build();
      HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
//      System.out.println("Response status code: " + response);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }


  @Override
  public Status getVertexCount() {
//    System.out.println("Get Vertex Count");
    boolean result = dbDriver.getVertexCount();
//    boolean result = dbDriver.executeQuery("MATCH (n) RETURN count(n)");
    if(!result){
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status getEdgeCount() {
//    System.out.println("Get Edge Count");
    boolean result = dbDriver.getEdgeCount();
//    boolean result = dbDriver.executeQuery("MATCH ()-[r]->() RETURN count(r)");
    if(!result){
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status getEdgeLabels() {
    boolean result = dbDriver.getEdgeLabels();
    if(!result){
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status getVertexWithProperty(String key, ByteIterator value) {
    boolean result = dbDriver.getVertexWithProperty(key, value.toString());

    //    boolean result = dbDriver.executeQuery("MATCH (n {"+key+": '"+value.toString()+"'}) RETURN n");
    if(!result) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status getEdgeWithProperty(String key, ByteIterator value) {
    boolean result = dbDriver.getEdgeWithProperty(key, value.toString());
//    boolean result = dbDriver.executeQuery("MATCH ()-[r {"+key+": '"+value.toString()+"'}]->() RETURN r");
    if(!result) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status getEdgesWithLabel(String label) {
    boolean result = dbDriver.getEdgesWithLabel(label);
//    boolean result = dbDriver.executeQuery("MATCH ()-[r:"+label+"]->() RETURN r");
    if(!result) {
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status setVertexProperty(String id, String key, ByteIterator value) {
   try{
      String query = "MATCH (n {id: '"+ id +"'}) SET n."+ key +" = '"+ value.toString() +"' RETURN n";
      String reqBody = "{ \"query\": \""+ query+"\", \"params\": null }";
      String targetString = props.getProperty("HOSTURI") + "/write";
        URI target = new URI(targetString);
        HttpRequest request = HttpRequest.newBuilder()
            .uri(target)
            .POST(HttpRequest.BodyPublishers.ofString(reqBody))
            .header("Content-Type", "application/json")
            .build();
        HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
//        System.out.println("Response status code: " + response);
        return Status.OK;
      } catch (Exception e) {
      return Status.ERROR;
   }
  }

  @Override
  public Status setEdgeProperty(String id, String key, ByteIterator value) {
    try{
        String query = "MATCH ()-[r {id: '"+ id +"'}]->() SET r."+ key +" = '"+ value.toString() +"' RETURN r";
        String reqBody = "{ \"query\": \""+ query+"\", \"params\": null }";
        String targetString = props.getProperty("HOSTURI") + "/write";
          URI target = new URI(targetString);
          HttpRequest request = HttpRequest.newBuilder()
              .uri(target)
              .POST(HttpRequest.BodyPublishers.ofString(reqBody))
              .header("Content-Type", "application/json")
              .build();
          HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
//          System.out.println("Response status code: " + response);
          return Status.OK;
        } catch (Exception e) {
        return Status.ERROR;
    }
  }

  @Override
  public Status removeVertex(String id) {
   try{
      String query = "MATCH (n {id: '"+ id +"'}) DETACH DELETE n";
      String reqBody = "{ \"query\": \""+ query+"\", \"params\": null }";
      String targetString = props.getProperty("HOSTURI") + "/write";
        URI target = new URI(targetString);
        HttpRequest request = HttpRequest.newBuilder()
            .uri(target)
            .POST(HttpRequest.BodyPublishers.ofString(reqBody))
            .header("Content-Type", "application/json")
            .build();
        HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
//        System.out.println("Response status code: " + response);
        return Status.OK;
      } catch (Exception e) {
      return Status.ERROR;
   }
  }

  @Override
  public Status removeEdge(String id) {
   try {
      String query = "MATCH ()-[r {id: '"+ id +"'}]->() DELETE r";
      String reqBody = "{ \"query\": \""+ query+"\", \"params\": null }";
      String targetString = props.getProperty("HOSTURI") + "/write";
        URI target = new URI(targetString);
        HttpRequest request = HttpRequest.newBuilder()
            .uri(target)
            .POST(HttpRequest.BodyPublishers.ofString(reqBody))
            .header("Content-Type", "application/json")
            .build();
        HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
//        System.out.println("Response status code: " + response);
        return Status.OK;
      } catch (Exception e) {
      return Status.ERROR;
   }
  }

  @Override
  public Status removeVertexProperty(String id, String key) {
    try{
      String query = "MATCH (n {id: '"+ id +"'}) REMOVE n."+ key +" RETURN n";
      String reqBody = "{ \"query\": \""+ query+"\", \"params\": null }";
      String targetString = props.getProperty("HOSTURI") + "/write";
        URI target = new URI(targetString);
        HttpRequest request = HttpRequest.newBuilder()
            .uri(target)
            .POST(HttpRequest.BodyPublishers.ofString(reqBody))
            .header("Content-Type", "application/json")
            .build();
        HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
//        System.out.println("Response status code: " + response);
        return Status.OK;
      } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status removeEdgeProperty(String id, String key) {
  try{
      String query = "MATCH ()-[r {id: '"+ id +"'}]->() REMOVE r."+ key +" RETURN r";
      String reqBody = "{ \"query\": \""+ query+"\", \"params\": null }";
      String targetString = props.getProperty("HOSTURI") + "/write";
        URI target = new URI(targetString);
        HttpRequest request = HttpRequest.newBuilder()
            .uri(target)
            .POST(HttpRequest.BodyPublishers.ofString(reqBody))
            .header("Content-Type", "application/json")
            .build();
        HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        System.out.println("Response status code: " + response);
        return Status.OK;
      } catch (Exception e) {
      return Status.ERROR;
  }
  }
}