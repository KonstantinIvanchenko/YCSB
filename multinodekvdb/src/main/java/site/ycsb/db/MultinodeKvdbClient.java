package site.ycsb.db;

import site.ycsb.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MultiNodeKVDB binding for YCSB framework.
 */
public class MultinodeKvdbClient extends DB {
  private static AtomicInteger INIT_COUNT = new AtomicInteger(0);
  private static final List<Integer> KVDBServerPortList = List.of(8020, 8021, 8022);
  //private static URI KVDBConnectionUriDefault;
  private static HttpClient KVDBClientDefault;

  //private static ArrayList<URI> KVDBConnectionUriList = new ArrayList<>();
  private static ArrayList<HttpClient> KVDBClientList = new ArrayList<>();

  private static Boolean RandomNodeSelect = false;

  private static final Integer MaxThreadsSingleClient = 3;
  private static final Integer MaxThreadsMultiClient = 2;

  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();

    if (KVDBClientDefault != null || KVDBClientList != null)
      return;

    if (RandomNodeSelect == false) {
      //httpclient create here
      ExecutorService ClientExecutor = Executors.newFixedThreadPool(MaxThreadsSingleClient);
      KVDBClientDefault = HttpClient.newBuilder().
          executor(ClientExecutor).
          build();
    }else{
      //httpclient create few client instances
      for(int i = 0; i < KVDBServerPortList.size(); i++) {
        ExecutorService ClientExecutor = Executors.newFixedThreadPool(MaxThreadsMultiClient);
        KVDBClientList.add(HttpClient.newBuilder().
            executor(ClientExecutor).
            build());
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    /*Tables are not supported. Thus merge table and key to get unique*/
    String KVDBkey = table + key;
    String keyDelimiter = "|";
    AtomicInteger counter = new AtomicInteger(0);
    Integer maxCounter = fields.size();

    if (RandomNodeSelect  == false) {
      //Work on client instances asynchronously for upsert requests; apply lambdas to process correct status ?
      for(String val : fields) {
        String getKey = val;
        //byte[] getValue;
        URI KVDBConnectionUri;

        try {
          //TODO:check replicas
          KVDBConnectionUri = new URI("http", "", "localhost",
              KVDBServerPortList.get(0), "/v0/entity",
              "id=" + KVDBkey + keyDelimiter + getKey, "&replicas=1/1");

          HttpRequest request = HttpRequest.newBuilder().
              uri(KVDBConnectionUri).
              GET().
              build();

          KVDBClientDefault.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).
              orTimeout(1, TimeUnit.SECONDS).
              whenComplete((response, error) -> {
                if (error == null && response.statusCode() == 200) {
                  byte[] RespBytes = response.body();
                  //String ResponseBody = Base64.getEncoder().encodeToString(RespBytes);
                  result.put(getKey, new ByteArrayByteIterator(RespBytes));
                  counter.getAndIncrement();
                }
              });

          long startTimeGet = System.currentTimeMillis();
          while (System.currentTimeMillis() - startTimeGet < 50) ;//50 msec
        } catch (URISyntaxException e) {
          System.out.println("Exc: KVDB invalid url error");
          return Status.ERROR;
          //System.exit(1);
        }
      }

      long startTimeGet = System.currentTimeMillis();
      while (counter.get() >= maxCounter
          &&
          (System.currentTimeMillis() - startTimeGet) < 500) ;//500 msec

      if (counter.get() >= maxCounter)
        return Status.OK;
      else
        return Status.NOT_FOUND;
    }else{

      int instance_idx = 0;
      int instance_num = KVDBServerPortList.size();

      //Work on client instances asynchronously for upsert requests; apply lambdas to process correct status ?
      for(String val : fields) {
        String getKey = val;
        //byte[] getValue;
        URI KVDBConnectionUri;

        instance_idx %= instance_num;

        try {
          //TODO:check replicas
          KVDBConnectionUri = new URI("http", "", "localhost",
              KVDBServerPortList.get(instance_idx), "/v0/entity",
              "id=" + KVDBkey + keyDelimiter + getKey, "&replicas=1/1");

          HttpRequest request = HttpRequest.newBuilder().
              uri(KVDBConnectionUri).
              GET().
              build();

          KVDBClientList.get(instance_idx).sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).
              orTimeout(1, TimeUnit.SECONDS).
              whenComplete((response, error) -> {
                if (error == null && response.statusCode() == 200) {
                  byte[] RespBytes = response.body();
                  //String ResponseBody = Base64.getEncoder().encodeToString(RespBytes);
                  result.put(getKey, new ByteArrayByteIterator(RespBytes));
                  counter.getAndIncrement();
                }
              });
        } catch (URISyntaxException e) {
          System.out.println("Exc: KVDB invalid url error");
          return Status.ERROR;
          //System.exit(1);
        }

        if (instance_idx == instance_num - 1) {
          long startTimeGet = System.currentTimeMillis();
          while (System.currentTimeMillis() - startTimeGet < 50) ;//50 msec
        }
      }

      long startTimeGet = System.currentTimeMillis();
      while (counter.get() >= maxCounter
          &&
          (System.currentTimeMillis() - startTimeGet) < 500) ;//500 msec
      if (counter.get() >= maxCounter)
        return Status.OK;
      else
        return Status.NOT_FOUND;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    //read(String table, String key, Set<String> fields, Map<String, ByteIterator> result)
    //result.ensureCapacity(recordcount);

    //FIXME: scan tests arenot possible on KVDB
    //Implement ?id=AllList0replicas=x/y endpoint at KVDB side; To collect required amount of key entries,
    //Key entry list size shall correponds to the recordcount*size(fields)
    //if matches, then implement scan
    //Otherwise stop;
    return null;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {

    //TODO: check this

    Set<String> allFields = values.keySet();
    Map<String, ByteIterator> resultDummy = new HashMap<String, ByteIterator>();
    //public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result)
    Status status = read(table, key, allFields, resultDummy);
    if (status == Status.OK)
      insert(table, key, values);

    return status;
  }

  /**
   *Insert values in the KVDB. Each value is stored under a unique key.
   * Each key is created as a string 'Table+key+"|"+value-key';
   *
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    /*Tables are not supported. Thus merge table and key to get unique*/
    String KVDBkey = table + key;
    String keyDelimiter = "|";
    AtomicInteger counter = new AtomicInteger(0);
    Integer maxCounter = values.size();

    if (RandomNodeSelect  == false) {

      //Work on client instances asynchronously for upsert requests; apply lambdas to process correct status ?
      for(Map.Entry<String, ByteIterator> val : values.entrySet()) {
        String getKey = val.getKey();
        byte[] getValue = val.getValue().toArray();
        URI KVDBConnectionUri;

        try {
          KVDBConnectionUri = new URI("http", "", "localhost",
              KVDBServerPortList.get(0), "/v0/entity",
              "id=" + KVDBkey + keyDelimiter + getKey, "&replicas=2/3");

          HttpRequest request = HttpRequest.newBuilder().
              uri(KVDBConnectionUri).
              PUT(HttpRequest.BodyPublishers.ofByteArray(getValue)).
              build();

          KVDBClientDefault.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).
              orTimeout(1, TimeUnit.SECONDS).
              whenComplete((response, error) -> {
                if (error == null && response.statusCode() == 201)
                  counter.getAndIncrement();
              });

          long startTimeGet = System.currentTimeMillis();
          while (System.currentTimeMillis() - startTimeGet < 50) ;//50 msec
        } catch (URISyntaxException e) {
          System.out.println("Exc: KVDB invalid url error");
          System.exit(1);
        }
      }

      long startTimeGet = System.currentTimeMillis();
      while (counter.get() >= maxCounter
          &&
          (System.currentTimeMillis() - startTimeGet) < 500) ;//500 msec

      if (counter.get() >= maxCounter)
        return Status.OK;
      else
        return Status.ERROR;
    }else{

      int instance_idx = 0;
      int instance_num = KVDBServerPortList.size();

      /*TBD - switch pseud0-randomly entry host*/
      //if there are many incoming valus, distribute requests over instances sequentially ?
      //Work on client instances asynchronously for upsert requests; apply lambdas to process correct status ?
      for(Map.Entry<String, ByteIterator> val : values.entrySet()) {
        String getKey = val.getKey();
        byte[] getValue = val.getValue().toArray();

        URI KVDBConnectionUri;

        instance_idx++;
        instance_idx %= instance_num;

        try {
          KVDBConnectionUri = new URI("http", "", "localhost",
              KVDBServerPortList.get(instance_idx), "/v0/entity",
              "id=" + KVDBkey + keyDelimiter + getKey, "&replicas=2/3");

          HttpRequest request = HttpRequest.newBuilder().
              uri(KVDBConnectionUri).
              PUT(HttpRequest.BodyPublishers.ofByteArray(getValue)).
              build();

          KVDBClientList.get(instance_idx).sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).
              orTimeout(1, TimeUnit.SECONDS).
              whenComplete((response, error) -> {
                if (error == null && response.statusCode() == 201)
                  counter.getAndIncrement();
              });
        } catch (URISyntaxException e) {
          System.out.println("Exc: KVDB invalid url error");
          System.exit(1);
        }

        if (instance_idx == instance_num - 1) {
          long startTimeGet = System.currentTimeMillis();
          while (System.currentTimeMillis() - startTimeGet < 50) ;//50 msec
        }
      }

      long startTimeGet = System.currentTimeMillis();
      while (counter.get() >= maxCounter
          &&
          (System.currentTimeMillis() - startTimeGet) < 500) ;//500 msec

      if (counter.get() >= maxCounter)
        return Status.OK;
      else
        return Status.ERROR;
    }
  }

  @Override
  public Status delete(String table, String key) {

    /*Tables are not supported. Thus merge table and key to get unique*/
    String KVDBkey = table + key;
    String keyDelimiter = "*";
    AtomicInteger counter = new AtomicInteger(0);
    Integer maxCounter = 1;

    URI KVDBConnectionUri;

    try {
      KVDBConnectionUri = new URI("http", "", "localhost",
          KVDBServerPortList.get(0), "/v0/entity",
          "id=" + KVDBkey + keyDelimiter, "&replicas=2/3");

      HttpRequest request = HttpRequest.newBuilder().
          uri(KVDBConnectionUri).
          DELETE().
          build();

      KVDBClientDefault.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).
          orTimeout(1, TimeUnit.SECONDS).
          whenComplete((response, error) -> {
            if (error == null && response.statusCode() == 202)
              counter.getAndIncrement();
          });
    } catch (URISyntaxException e) {
      System.out.println("Exc: KVDB invalid url error");
      System.exit(1);
    }

    long startTimeGet = System.currentTimeMillis();
    while (counter.get() >= maxCounter
        &&
        (System.currentTimeMillis() - startTimeGet) < 500) ;//500 msec

    if (counter.get() >= maxCounter)
      return Status.OK;
    else
      return Status.ERROR;
  }
}
