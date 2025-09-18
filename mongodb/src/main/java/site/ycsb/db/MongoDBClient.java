package site.ycsb.db;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.result.InsertOneResult;

import org.bson.Document;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class MongoDBClient extends DB {

  private static Properties props = new Properties();
  MongoClient mongoClient;

  @Override
  public void init() throws DBException {
    System.out.println("Starting Mongodb client");
    props = getProperties();
    System.out.println(props);

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
    try{
      this.mongoClient = MongoClients.create(
          MongoClientSettings.builder()
              .applyConnectionString(new ConnectionString(props.getProperty("DBURI")))
              .readConcern(ReadConcern.LOCAL)
              .writeConcern(com.mongodb.WriteConcern.MAJORITY)
              .build()
      );

      // Create a database and a vertex and edge collection if they don't exist
      mongoClient.getDatabase("grace").createCollection("vertices");
      mongoClient.getDatabase("grace").createCollection("edges");

      // Test connection status and print it

      mongoClient.listDatabaseNames().first();
      System.out.println("Connected to MongoDB database successfully");
    } catch (Exception e) {
      System.out.println("Failed to connect to MongoDB database: " + e.getMessage());
      throw new DBException(e);
    }
  }


  @Override
  public Status addVertex(String label, String id, Map<String, String> properties) {
//    System.out.println("Add Vertex");
//    return Status.OK;
    int status = 0;

    try {
//      System.out.println("Adding vertex with id: " + id);
      Document vertex = new Document();
      vertex.append("label", label);
      vertex.append("id", id);
      Document props = new Document();
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        props.append(entry.getKey(), entry.getValue().toString());
      }
      vertex.append("properties", props);
      InsertOneResult result = this.mongoClient.getDatabase("grace").getCollection("vertices").insertOne(vertex);
//      System.out.println("Inserted vertex with id: " + result);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }
  @Override
  public Status addEdge(String label, String id, String from, String to, Map<String, String> properties) {
    int status = 0;
    try {
      Document edge = new Document();
      edge.append("label", label);
      edge.append("id", id);
      edge.append("from", from);
      edge.append("to", to);
      Document props = new Document();
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        props.append(entry.getKey(), entry.getValue().toString());
      }
      edge.append("properties", props);
      InsertOneResult result = this.mongoClient.getDatabase("grace").getCollection("edges").insertOne(edge);
//      System.out.println("Inserted edge with id: " + result);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }

  }


  @Override
  public Status setVertexProperty(String id, String key, String value) {
    try{
      Document vertex = this.mongoClient.getDatabase("grace").getCollection("vertices").find(new Document("id", id)).first();
      if(vertex==null){
        return Status.ERROR;
      }
      Document props = (Document) vertex.get("properties");
      if(props==null){
        props = new Document();
      }
      props.append(key, value.toString());
      vertex.put("properties", props);
      this.mongoClient.getDatabase("grace").getCollection("vertices").replaceOne(new Document("id", id), vertex);
      return Status.OK;
    } catch (Exception e){
      return Status.ERROR;
    }
  }

  @Override
  public Status setEdgeProperty(String id, String key, String value) {
    try {
      Document edge = this.mongoClient.getDatabase("grace").getCollection("edges").find(new Document("id", id)).first();
      if (edge == null) {
        return Status.ERROR;
      }
      Document props = (Document) edge.get("properties");
      if (props == null) {
        props = new Document();
      }
      props.append(key, value.toString());
      edge.put("properties", props);
      this.mongoClient.getDatabase("grace").getCollection("edges").replaceOne(new Document("id", id), edge);
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status removeVertex(String id) {
    int status = 0;
    try {
      this.mongoClient.getDatabase("grace").getCollection("vertices").deleteOne(new Document("id", id));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status removeEdge(String id) {
    int status = 0;
    try {
      this.mongoClient.getDatabase("grace").getCollection("edges").deleteOne(new Document("id", id));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status removeVertexProperty(String id, String key) {
    try{
      Document vertex = this.mongoClient.getDatabase("grace").getCollection("vertices").find(new Document("id", id)).first();
      if(vertex==null){
        return Status.ERROR;
      }
      Document props = (Document) vertex.get("properties");
      if(props==null || !props.containsKey(key)){
        return Status.ERROR;
      }
      props.remove(key);
      vertex.put("properties", props);
      this.mongoClient.getDatabase("grace").getCollection("vertices").replaceOne(new Document("id", id), vertex);
      return Status.OK;
    }catch (Exception e){
      return Status.ERROR;
    }
  }

  @Override
  public Status removeEdgeProperty(String id, String key) {
    try{
      Document edge = this.mongoClient.getDatabase("grace").getCollection("edges").find(new Document("id", id)).first();
      if(edge==null){
        return Status.ERROR;
      }
      Document props = (Document) edge.get("properties");
      if(props==null || !props.containsKey(key)){
        return Status.ERROR;
      }
      props.remove(key);
      edge.put("properties", props);
      this.mongoClient.getDatabase("grace").getCollection("edges").replaceOne(new Document("id", id), edge);
      return Status.OK;
    }catch (Exception e){
      return Status.ERROR;
    }
  }


  @Override
  public Status getVertexCount() {
    try{
      Long res = this.mongoClient.getDatabase("grace").getCollection("vertices").countDocuments();
//      System.out.println("Vertex count: " + res);
    } catch (Exception e){
      System.out.println("Exception in getVertexCount: " + e.getMessage());
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status getEdgeCount() {
    try{
      this.mongoClient.getDatabase("grace").getCollection("edges").countDocuments();
    } catch (Exception e){
      System.out.println("Exception in getEdgeCount: " + e.getMessage());
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status getEdgeLabels() {
    try{
      this.mongoClient.getDatabase("grace").getCollection("edges").distinct("label", String.class).first();
    } catch (Exception e){
      System.out.println("Exception in getEdgeLabels: " + e.getMessage());
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status getVertexWithProperty(String key, String value) {
    try{
      AtomicLong count= new AtomicLong();
      System.out.println("Searching for vertices with " + key + " = " + value);
      this.mongoClient.getDatabase("grace").getCollection("vertices").find(new org.bson.Document(key, value)).forEach(doc -> {
        // Print the document
        System.out.println(doc.toJson());
        count.getAndIncrement();
      });

    } catch (Exception e){
      System.out.println("Exception in getVertexWithProperty: " + e.getMessage());
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public  Status getEdgeWithProperty(String key, String value) {
    try{
      FindIterable<Document> result = this.mongoClient.getDatabase("grace").getCollection("edges").find(new Document(key, value.toString()));
      for (Document doc : result) {
        doc.toJson();
      }
    } catch (Exception e){
      System.out.println("Exception in getEdgeWithProperty: " + e.getMessage());
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status getEdgesWithLabel(String label) {
    try{
      FindIterable<Document> result = this.mongoClient.getDatabase("grace").getCollection("edges").find(new Document("label", label));
      for (Document doc : result) {

      }
    } catch (Exception e){
      System.out.println("Exception in getEdgesWithLabel: " + e.getMessage());
      return Status.ERROR;
    }
    return Status.OK;
  }
}
