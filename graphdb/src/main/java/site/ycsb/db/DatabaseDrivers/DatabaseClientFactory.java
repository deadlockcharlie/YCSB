package site.ycsb.db.DatabaseDrivers;

import java.util.Map;

public class DatabaseClientFactory {

  private static final Map<String, DatabaseClient> clients = Map.of(
      "neo4j", new Neo4jDriver(),
      "memgraph", new Neo4jDriver()
//      "janusgraph", new JanusGraphDriver()
  );

  public static DatabaseClient getDatabaseClient(String name) {
    DatabaseClient client = clients.get(name.toLowerCase());
    if (client == null) {
      throw new IllegalArgumentException("Unsupported database: " + name);
    }
    return client;
  }

}
