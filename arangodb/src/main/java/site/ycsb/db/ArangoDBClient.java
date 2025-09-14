package site.ycsb.db;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.model.CollectionCreateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;


import java.net.URI;
import java.util.*;

public class ArangoDBClient extends DB {

  private static Properties props = new Properties();
  private final Logger log = LoggerFactory.getLogger(getClass());
  ArangoDatabase db;


  @Override
  public void init() throws DBException {
    try {
      System.out.println("Starting ArangoDB client");
      props = getProperties();
      System.out.println(props);

      if (props.getProperty("DBTYPE") == null) {
        System.out.println("DBTYPE must be provided");
        throw new DBException("DBTYPE must be provided");
      }
      System.out.println("DBTYPE: " + props.getProperty("DBTYPE"));

      if (props.getProperty("DBURI") == null) {
        System.out.println("DBURI must be provided");
        throw new DBException("DBURI must be provided");
      }
      System.out.println("DBURI: " + props.getProperty("DBURI"));

      URI uri = new URI(props.getProperty("DBURI"));

      System.out.println("Connecting to ArangoDB database with params: " + uri.getHost() + ":" + uri.getPort());
      ArangoDB arangodb = new ArangoDB.Builder()
          .host(uri.getHost(), uri.getPort())
          .build();
      System.out.println("ArangoDB client initialized successfully");
      this.db = arangodb.db("ycsb");
      if (!db.exists()) {
        arangodb.createDatabase("ycsb");
        this.db = arangodb.db("ycsb");
        int replicationFactor = props.getProperty("REPLICATION_FACTOR") != null ? Integer.parseInt(props.getProperty("REPLICATION_FACTOR")) : 1;
        // Set collection options
        CollectionCreateOptions options = new CollectionCreateOptions()
            .replicationFactor(replicationFactor)  // replication factor of 3
            .writeConcern(replicationFactor);      // optional, how many copies must be written



        db.createCollection("vertices", options);
        db.createCollection("edges", options);
      }
      System.out.println("Connected to ArangoDB database successfully");

    }catch (Exception e){
      System.out.println("Failed to initialize ArangoDB client: " + e.getMessage());
      throw new DBException(e);
    }
  }

  @Override
  public Status addVertex(String label, String id, Map<String, ByteIterator> properties) {
    int status = 0;
    try {
      Map<String, String> vertex = new HashMap<>();

      vertex.put("_key", id);
      for (Map.Entry<String, ByteIterator> entry : properties.entrySet()) {
        vertex.put(entry.getKey(), entry.getValue().toString());
      }
      db.collection("vertices").insertDocument(vertex);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }
  @Override
  public Status addEdge(String label, String id, String from, String to, Map<String, ByteIterator> properties) {
    int status = 0;
    try {
      Map<String, String> edge = new HashMap<>();
      edge.put("_key", id);
      edge.put("_from", "vertices/" + from);
      edge.put("_to", "vertices/" + to);
      for (Map.Entry<String, ByteIterator> entry : properties.entrySet()) {
        edge.put(entry.getKey(), entry.getValue().toString());
      }
      db.collection("edges").insertDocument(edge);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }

  }
  @Override
  public Status getVertexCount() {
    try {
      db.collection("vertices").count();
      return Status.OK;
    } catch (Exception e) {
      System.out.println("Exception in getVertexCount: " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status getEdgeCount() {
    try {
      db.collection("edges").count();
      return Status.OK;
    } catch (Exception e) {
      System.out.println("Exception in getEdgeCount: " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status getEdgeLabels() {
    try{
      db.query("FOR e IN edges COLLECT label = e._key RETURN label", String.class).asListRemaining();
      return Status.OK;
    } catch (Exception e) {
      System.out.println("Exception in getEdgeLabels: " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status getVertexWithProperty(String key, ByteIterator value) {
    try{
      String query = "FOR v IN vertices FILTER v."+key+" ==\" "+value.toString()+"\" RETURN v";
      ArangoCursor<String> cursor = db.query(query, String.class);
      return Status.OK;
    } catch (Exception e) {
      System.out.println("Exception in getVertexWithProperty: " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status getEdgeWithProperty(String key, ByteIterator value) {
    try {
      String query = "FOR e IN edges FILTER e."+key+" ==\" "+value.toString()+"\" RETURN e";
//      System.out.println(query);
      ArangoCursor<String> cursor = db.query(query, String.class);

//      System.out.println("Query executed: " + cursor.asListRemaining());
      return Status.OK;
    } catch (Exception e) {
      System.out.println("Exception in getEdgeWithProperty: " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status getEdgesWithLabel(String label) {
    try {
      String query = "FOR e IN edges FILTER e.label == [\"YCSBEdge\"] RETURN e";

      ArangoCursor<String> cursor = db.query(query, String.class);

//      System.out.println("Query executed: " + cursor.asListRemaining());
      return Status.OK;
    } catch (Exception e) {
      System.out.println("Exception in getEdgesWithLabel: " + e.getMessage());
      return Status.ERROR;
    }
  }

  @Override
  public Status setVertexProperty(String id, String key, ByteIterator value) {
    try{
      Map vertex = db.collection("vertices").getDocument(id, Map.class);
      vertex.put(key, value.toString());
      db.collection("vertices").updateDocument(id, vertex);
      return Status.OK;
    } catch (Exception e){
      return Status.ERROR;
    }
  }

  @Override
  public Status setEdgeProperty(String id, String key, ByteIterator value) {
    try {
      Map edge = db.collection("edges").getDocument(id, Map.class);
      edge.put(key, value.toString());
      db.collection("edges").updateDocument(id, edge);

        return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status removeVertex(String id) {
    int status = 0;
    try {

      db.collection("vertices").deleteDocument(id);
        return Status.OK;

    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status removeEdge(String id) {
    int status = 0;
    try {
      db.collection("edges").deleteDocument(id);
        return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status removeVertexProperty(String id, String key) {
    try{
      Map vertex = db.collection("vertices").getDocument(id, Map.class);
      vertex.remove(key);
      db.collection("vertices").updateDocument(id, vertex);
     return Status.OK;
  }catch (Exception e){
      return Status.ERROR;
    }
  }

  @Override
  public Status removeEdgeProperty(String id, String key) {
    try{
      Map edge = db.collection("edges").getDocument(id, Map.class);
      edge.remove(key);
      db.collection("edges").updateDocument(id, edge);
      return Status.OK;
    }catch (Exception e){
      return Status.ERROR;
    }
  }


//
//  @Override
//  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
//    return Status.OK;
//  }
//
//  @Override
//  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
//    try {
//      String reqBody = " { \"limit\": \"23\"}";
//      String targetString = props.getProperty("HOSTURI") + "/api/getGraph";
////      System.out.println(targetString);
//      URI target = new URI(targetString);
//      HttpRequest request = HttpRequest.newBuilder()
//          .uri(target)
//          .GET()
//          .header("Content-Type", "application/json")
//          .build();
//      HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
//      if(response.statusCode() == 200 || response.statusCode() == 201){
//        return Status.OK;
//      } else{
//        return Status.ERROR;
//      }
//    } catch (Exception e){
////      System.out.println(e.getMessage());
//      return Status.ERROR;
//    }
//  }
//
//  // CAUTION. WE need a delete API for the database we are testing. YCSB by default does not execute deletes.
//  // The delete operation is implemented as an update, which is sent to the database.
//  @Override
//  public Status update(String table, String key, Map<String, ByteIterator> values) {
//    if (!(Vertices.isEmpty())) {
//      int size = Vertices.size();
//      int item = new Random().nextInt(size); // In real life, the Random object should be rather more shared than this
//      int i = 0;
//      String deleteVertex = "";
//      for (String vert : Vertices) {
//        if (i == item) {
//          deleteVertex = vert;
//          break;
//        }
//        i++;
//      }
//      if(Objects.equals(deleteVertex, "")){
//        return Status.ERROR;
//      }
//
//      try {
//        String reqBody = " { \"label\": \"ProductItem\", \"properties\": {\"identifier\":\"" + deleteVertex + "\"}}";
//        String targetString = props.getProperty("HOSTURI") + "/api/deleteVertex";
////        System.out.println(targetString);
//        URI target = new URI(targetString);
//        HttpRequest request = HttpRequest.newBuilder()
//            .uri(target)
//            .POST(HttpRequest.BodyPublishers.ofString(reqBody))
//            .header("Content-Type", "application/json")
//            .build();
//        HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
//        if(response.statusCode()==200 || response.statusCode()==201){
//          Vertices.remove(deleteVertex);
//          return Status.OK;
//
//        } else {
//          return Status.ERROR;
//        }
//      } catch (Exception e) {
////        System.out.println(e.getMessage());
//        return Status.ERROR;
//      }
//    }
//    return Status.OK;
//  }
//
//  @Override
//  public Status insert(String table, String key, Map<String, ByteIterator> values) {
//    int status = 0;
//    try {
//      String edge = reader.readLine();
//      String[] vertices = edge.split(" ");
//      for (String vertex : vertices) {
////        System.out.println("Insert "+vertex);
//        if (!Vertices.contains(vertex)) {
//          String reqBody = " { \"label\": \"ProductItem\", \"properties\": {\"identifier\":\"" + vertex + "\",\"name\": \"Vertex" + vertex + "\" }}";
//          String targetString= props.getProperty("HOSTURI") + "/api/addVertex";
////          System.out.println(targetString);
//          URI target = new URI(targetString);
//          HttpRequest request = HttpRequest.newBuilder()
//              .uri(target)
//              .POST(HttpRequest.BodyPublishers.ofString(reqBody))
//              .header("Content-Type", "application/json")
//              .build();
//          HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
//          status+= response.statusCode();
//        }
//      }
//
//      // We receive responses for two requests. The success codes are 200 and 201. The sum of these should be less than 500 always.
//      // If either request is 500, this sum would be greater than 500 at least. so, we do not execute the edge add.
//      if(status<500) {
//        Vertices.add(vertices[0]);
//        Vertices.add(vertices[1]);
//        String reqBody = " { \"sourceLabel\": \"ProductItem\"" +
//                            ",\"sourcePropName\": \"identifier\"" +
//                            ",\"sourcePropValue\":\"" + vertices[0] +"\""+
//                            ",\"targetLabel\": \"ProductItem\"" +
//                            ",\"targetPropName\": \"identifier\"" +
//                            ",\"targetPropValue\":\"" + vertices[1] +"\""+
//                            ",\"relationType\": \"SAME_CATEGORY\"" +
//                            ",\"properties\": {\"identifier\":\"" + vertices[0]+"-"+vertices[1]  + "\",\"relation\": [\"recommendation\"] } }";
//        String targetString= props.getProperty("HOSTURI") + "/api/addEdge";
////        System.out.println(targetString);
//        URI target = new URI(targetString);
//
//        HttpRequest request = HttpRequest.newBuilder()
//            .uri(target)
//            .POST(HttpRequest.BodyPublishers.ofString(reqBody))
//            .header("Content-Type", "application/json")
//            .build();
//
//        HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
////        System.out.println(response.statusCode());
////        System.out.println(response.body().toString());
//        if(response.statusCode()==500) {
//          return Status.ERROR;
//        }
//        Edges.add( vertices[0]+"-"+vertices[1]);
//      }
//
//      return Status.OK;
//
//    } catch (IOException e) {
//      return Status.ERROR;
//    } catch (InterruptedException | URISyntaxException e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  @Override
//  public Status delete(String table, String key) {
//    return Status.OK;
//  }

}