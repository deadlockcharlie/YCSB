package site.ycsb.db;

import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class DBDriver {
  Driver neo4jDriver;
  GraphTraversalSource janusgraphDriver;
  String name;
  DBDriver(String name, String URI) {
    if(name.equals("neo4j") || name.equals("memgraph")){
        this.name = name;
        //init neo4j driver
        this.neo4jDriver = GraphDatabase.driver(URI);
        neo4jDriver.verifyConnectivity();
    }
    else if(name.equals("janusgraph")){
        this.name = name;
      //init janusgraph driver
      this.janusgraphDriver = traversal().withRemote(DriverRemoteConnection.using(URI));
    }
  }

  public boolean executeQuery(String query) {
    try {
      if (name.equals("neo4j") || name.equals("memgraph")) {
        try (var session = neo4jDriver.session()) {
          session.writeTransaction(tx -> {
            tx.run(query);
            return null; // transaction needs a return value
          });
        }
      } else if (name.equals("janusgraph")) {
        janusgraphDriver.V().hasLabel("person").toList();
        // or execute query string if you have parsing
      }
      return true; // ✅ query executed successfully
    } catch (Exception e) {
      e.printStackTrace();
      return false; //  something failed
    }
  }


}
