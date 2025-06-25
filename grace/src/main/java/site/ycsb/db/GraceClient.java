package site.ycsb.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;

public class GraceClient extends DB {

  private static HttpClient httpClient;
  private static BufferedReader reader;
  private static FileReader fileReader;
  private static Properties props = new Properties();
  private final Logger log = LoggerFactory.getLogger(getClass());

  private Set<String> Vertices = new HashSet<>();
  private Set<String> Edges = new HashSet<>();

  public GraceClient() {
    System.out.println("GraceClient  Created");
  }

  @Override
  public void init() throws DBException {
    System.out.println("Starting GRACE client");
    props = getProperties();
    System.out.println(props);

    httpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .followRedirects(HttpClient.Redirect.NORMAL)
        .connectTimeout(Duration.ofSeconds(5))
        .build();

    try{
      String datafilepath = props.getProperty("DATAFILE");
      if(datafilepath == null){
        throw new RuntimeException("Missing or incorrect datafile path");
      }
      reader = new BufferedReader(new FileReader(datafilepath));
    }catch(Exception e){
      System.out.println(e.getMessage());
    }

  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    try {
      String reqBody = " { \"limit\": \"23\"}";
      String targetString = props.getProperty("HOSTURI") + "/api/getGraph";
//      System.out.println(targetString);
      URI target = new URI(targetString);
      HttpRequest request = HttpRequest.newBuilder()
          .uri(target)
          .GET()
          .header("Content-Type", "application/json")
          .build();
      HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
      if(response.statusCode() == 200 || response.statusCode() == 201){
        return Status.OK;
      } else{
        return Status.ERROR;
      }
    } catch (Exception e){
//      System.out.println(e.getMessage());
      return Status.ERROR;
    }
  }

  // CAUTION. WE need a delete API for the database we are testing. YCSB by default does not execute deletes.
  // The delete operation is implemented as an update, which is sent to the database.
  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    if (!(Vertices.isEmpty())) {
      int size = Vertices.size();
      int item = new Random().nextInt(size); // In real life, the Random object should be rather more shared than this
      int i = 0;
      String deleteVertex = "";
      for (String vert : Vertices) {
        if (i == item) {
          deleteVertex = vert;
          break;
        }
        i++;
      }
      if(Objects.equals(deleteVertex, "")){
        return Status.ERROR;
      }

      try {
        String reqBody = " { \"label\": \"ProductItem\", \"properties\": {\"identifier\":\"" + deleteVertex + "\"}}";
        String targetString = props.getProperty("HOSTURI") + "/api/deleteVertex";
//        System.out.println(targetString);
        URI target = new URI(targetString);
        HttpRequest request = HttpRequest.newBuilder()
            .uri(target)
            .POST(HttpRequest.BodyPublishers.ofString(reqBody))
            .header("Content-Type", "application/json")
            .build();
        HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        if(response.statusCode()==200 || response.statusCode()==201){
          Vertices.remove(deleteVertex);
          return Status.OK;

        } else {
          return Status.ERROR;
        }
      } catch (Exception e) {
//        System.out.println(e.getMessage());
        return Status.ERROR;
      }
    }
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    int status = 0;
    try {
      String edge = reader.readLine();
      String[] vertices = edge.split(" ");
      for (String vertex : vertices) {
//        System.out.println("Insert "+vertex);
        if (!Vertices.contains(vertex)) {
          String reqBody = " { \"label\": \"ProductItem\", \"properties\": {\"identifier\":\"" + vertex + "\",\"name\": \"Vertex" + vertex + "\" }}";
          String targetString= props.getProperty("HOSTURI") + "/api/addVertex";
//          System.out.println(targetString);
          URI target = new URI(targetString);
          HttpRequest request = HttpRequest.newBuilder()
              .uri(target)
              .POST(HttpRequest.BodyPublishers.ofString(reqBody))
              .header("Content-Type", "application/json")
              .build();
          HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
          status+= response.statusCode();
        }
      }

      // We receive responses for two requests. The success codes are 200 and 201. The sum of these should be less than 500 always.
      // If either request is 500, this sum would be greater than 500 at least. so, we do not execute the edge add.
      if(status<500) {
        Vertices.add(vertices[0]);
        Vertices.add(vertices[1]);
        String reqBody = " { \"sourceLabel\": \"ProductItem\"" +
                            ",\"sourcePropName\": \"identifier\"" +
                            ",\"sourcePropValue\":\"" + vertices[0] +"\""+
                            ",\"targetLabel\": \"ProductItem\"" +
                            ",\"targetPropName\": \"identifier\"" +
                            ",\"targetPropValue\":\"" + vertices[1] +"\""+
                            ",\"relationType\": \"SAME_CATEGORY\"" +
                            ",\"properties\": {\"identifier\":\"" + vertices[0]+"-"+vertices[1]  + "\",\"relation\": [\"recommendation\"] } }";
        String targetString= props.getProperty("HOSTURI") + "/api/addEdge";
//        System.out.println(targetString);
        URI target = new URI(targetString);

        HttpRequest request = HttpRequest.newBuilder()
            .uri(target)
            .POST(HttpRequest.BodyPublishers.ofString(reqBody))
            .header("Content-Type", "application/json")
            .build();

        HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
//        System.out.println(response.statusCode());
//        System.out.println(response.body().toString());
        if(response.statusCode()==500) {
          return Status.ERROR;
        }
        Edges.add( vertices[0]+"-"+vertices[1]);
      }

      return Status.OK;

    } catch (IOException e) {
      return Status.ERROR;
    } catch (InterruptedException | URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

}