package tl.util;

import org.neo4j.driver.v1.*;

import java.util.List;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jDriver implements AutoCloseable {
    private final Driver driver;
    private final Session session;

    public Neo4jDriver(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        session = driver.session();
    }

    @Override
    public void close() throws Exception {
        session.close();
        driver.close();
    }

//    public List<Record> allShortestPath(final long id1, final long id2) {
//        try (Session session = driver.session()) {
//            List<Record> results = session.writeTransaction(new TransactionWork<List<Record>>() {
//                @Override
//                public List<Record> execute(Transaction tx) {
//                    StatementResult result = tx.run("MATCH p=allShortestPaths((n:Person)-[*..6]-(m:Person))\n" +
//                                    "WHERE id(n)=$id1 and id(m)=$id2\n" +
//                                    "RETURN p",
//                            parameters("id1", id1, "id2", id2));
//                    return result.list();
//                }
//            });
//            return results;
//        }
//    }

    public String getUid(final long id) {
//        Session session = driver.session();
        StatementResult result = session.run("MATCH (n:Person) WHERE id(n)=$id RETURN n.uid", parameters("id", id));
//        session.close();
        return result.single().get(0).asString();
    }
}
