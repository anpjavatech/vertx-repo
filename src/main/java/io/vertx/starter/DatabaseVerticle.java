package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class DatabaseVerticle extends AbstractVerticle {

  public static final String CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url";
  public static final String CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class";
  public static final String CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size";
  public static final String CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueries.resource.file";

  public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseVerticle.class);

  private enum SqlQuery {
    CREATE_PAGES_TABLE,
    ALL_PAGES,
    GET_PAGES,
    CREATE_PAGE,
    SAVE_PAGE,
    DELETE_PAGE
  }

  private final HashMap<SqlQuery, String> sqlQueries = new HashMap<>();

  private JDBCClient jdbcClient;

  private void loadSqlQueries() throws IOException {

    String queriesFile = config().getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE);
    InputStream inputStream;

    if (queriesFile != null) {
      inputStream = new FileInputStream(queriesFile);
    } else {
      inputStream = getClass().getResourceAsStream("/db-queries.properties");
    }

    Properties queriesProps = new Properties();
    queriesProps.load(inputStream);
    inputStream.close();

    sqlQueries.put(SqlQuery.CREATE_PAGES_TABLE, queriesProps.getProperty("create_pages_table"));
    sqlQueries.put(SqlQuery.ALL_PAGES, queriesProps.getProperty("all_pages"));
    sqlQueries.put(SqlQuery.GET_PAGES, queriesProps.getProperty("get_page"));
    sqlQueries.put(SqlQuery.CREATE_PAGE, queriesProps.getProperty("create_page"));
    sqlQueries.put(SqlQuery.SAVE_PAGE, queriesProps.getProperty("save_page"));
    sqlQueries.put(SqlQuery.DELETE_PAGE, queriesProps.getProperty("delete_page"));
  }

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    loadSqlQueries();
    jdbcClient = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", config().getString(CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:file:db/wiki"))
      .put("driver_class", config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver"))
      .put("max_pool_size", config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 30)));

    jdbcClient.getConnection(ar -> {
      if (ar.succeeded()) {
        LOGGER.info("Connection to Database successful.");
        SQLConnection connection = ar.result();
        connection.execute(sqlQueries.get(SqlQuery.GET_PAGES), list ->{
          if(list.succeeded()){
            vertx.eventBus().consumer(config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue"), this::onMessage);
            startFuture.complete();

          }else {
            LOGGER.error("Exception while executing query.", list.cause());
            startFuture.fail(list.cause());
          }
        });

      } else {
        LOGGER.error("Connection not successful.", ar.cause());
        startFuture.fail(ar.cause());
      }
    });
  }

  public enum ErrorCodes{
    NO_ACTION_SPECIFIED,
    BAD_ACTION,
    DB_ERROR
  }

  private void onMessage(Message<JsonObject> message) {

    if(!message.headers().contains("action")){
      LOGGER.error("No action Header available.");
      message.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action header specified");
      return;
    }
    String action = message.headers().get("action");

    switch (action){
      case "list":
        fetchAllPages(message);
        break;
      case "create":
        createPage(message);
        break;
      default:
        message.fail(ErrorCodes.BAD_ACTION.ordinal(), "Bad action : "+action);

    }

  }

  private void createPage(Message<JsonObject> message) {

  }

  private void fetchAllPages(Message<JsonObject> message) {

  }
}
