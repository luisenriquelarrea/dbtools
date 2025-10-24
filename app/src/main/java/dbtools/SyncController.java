package dbtools;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import io.github.cdimascio.dotenv.Dotenv;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SyncController {

    @FXML private Button getFavsButton;
    @FXML private Button getUpdatesButton;
    @FXML private Button openConnButton;
    @FXML private Button closeConnButton;
    @FXML private Label connStatusLabel;
    @FXML private ComboBox<String> tableSelector;
    @FXML private Button syncStructButton;
    @FXML private Button syncDataButton;
    @FXML private TextArea logArea;

    private final Dotenv dotenv = Dotenv.load();

    private boolean sshOk = false;
    private boolean dbSrcOk = false;
    private boolean dbDestOk = false;

    private Session sshSession = null;
    
    private Connection srcConn = null;
    private Connection destConn = null;

    private volatile boolean isBusy = false;

    // Called automatically after FXML loads
    @FXML
    public void initialize() {
        log("Ready. Please validate SSH and DB connections.");
    }

    public void closeConnections() {
        if (isBusy){
            log("Still working...");
            return;
        }
        isBusy = true;
        try {
            if (srcConn != null && !srcConn.isClosed()) {
                srcConn.close();
                log("Remote DB connection closed.");
            }
        } catch (Exception e) {
            log("Error closing remote DB: " + e.getMessage());
        }
    
        try {
            if (destConn != null && !destConn.isClosed()) {
                destConn.close();
                log("Localhost DB connection closed.");
            }
        } catch (Exception e) {
            log("Error closing localhost DB: " + e.getMessage());
        }
    
        try {
            if (sshSession != null && sshSession.isConnected()) {
                sshSession.disconnect();
                log("SSH session disconnected.");
            }
        } catch (Exception e) {
            log("Error closing SSH session: " + e.getMessage());
        }

        srcConn = null;
        destConn = null;
        sshSession = null;

        sshOk = false;
        dbSrcOk = false;
        dbDestOk = false;

        Platform.runLater(() -> {
            connStatusLabel.setText("Disconnected");
            connStatusLabel.setStyle("-fx-text-fill: gray;");

            tableSelector.getItems().clear();

            disableActionButtons(true);

            isBusy = false;
        });
    }

    @FXML
    private void handleGetUpdates() {
        if (isBusy) {
            log("Still working...");
            return;
        }
        isBusy = true;
        log("Checking for new tables in source...");

        new Thread(() -> {
            try {
                if (srcConn == null || destConn == null || srcConn.isClosed() || destConn.isClosed()) {
                    Platform.runLater(() -> {
                        log("Connections are not open. Please reconnect first.");
                        isBusy = false;
                    });
                    return;
                }
                List<String> srcTables = getTableList(srcConn);
                List<String> destTables = getTableList(destConn);

                List<String> newTables = new ArrayList<>();
                for (String t : srcTables) {
                    if (!destTables.contains(t)) {
                        newTables.add(t);
                    }
                }

                if (newTables.isEmpty()) {
                    Platform.runLater(() -> log("No new tables found."));
                    return;
                }

                Platform.runLater(() -> log("Found " + newTables.size() + " new table(s): " + newTables));

                for (String table : newTables) {
                    try (Statement stmtSrc = srcConn.createStatement();
                        ResultSet rs = stmtSrc.executeQuery("SHOW CREATE TABLE `" + table + "`")) {
                        if (rs.next()) {
                            String createSQL = rs.getString(2);

                            try (Statement stmtDest = destConn.createStatement()) {
                                stmtDest.execute(createSQL);
                                Platform.runLater(() -> log("Created table: " + table));
                            }
                        }
                    } catch (SQLException e) {
                        Platform.runLater(() -> log("Failed to create table " + table + ": " + e.getMessage()));
                    }
                }

                Platform.runLater(() -> log("New tables synchronization complete."));

            } catch (Exception e) {
                Platform.runLater(() -> log("Error syncing new tables: " + e.getMessage()));
            } finally {
                Platform.runLater(() -> isBusy = false);
            }
        }).start();
    }


    @FXML
    private void openConnections() {
        if (isBusy){
            log("Still working...");
            return;
        }
        isBusy = true;
        new Thread(() -> {
            try {
                // --- 1. SSH ---
                String sshHost = dotenv.get("SSH_HOST");
                String sshUser = dotenv.get("SSH_USER");
                String sshPass = dotenv.get("SSH_PASS");
                int sshPort = Integer.parseInt(dotenv.get("SSH_PORT"));
                int localPort = Integer.parseInt(dotenv.get("DB_SRC_AVALIABLE_PORT"));
                int remotePort = Integer.parseInt(dotenv.get("DB_SRC_PORT"));
                String remoteHost = dotenv.get("DB_SRC_HOST");

                JSch jsch = new JSch();
                sshSession = jsch.getSession(sshUser, sshHost, sshPort);
                sshSession.setPassword(sshPass);
                sshSession.setConfig("StrictHostKeyChecking", "no");
                sshSession.connect(5000);
                sshSession.setPortForwardingL(localPort, remoteHost, remotePort);
                sshOk = true;
                Platform.runLater(() -> log("SSH connection successful."));

                // --- 2. Source DB (through SSH tunnel) ---
                String dbHost = dotenv.get("DB_SRC_HOST");
                String dbName = dotenv.get("DB_SRC_NAME");
                String dbUser = dotenv.get("DB_SRC_USER");
                String dbPass = dotenv.get("DB_SRC_PASS");
                String jdbcUrlSrc = "jdbc:mysql://"+ dbHost + ":" + localPort + "/" + dbName
                        + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

                srcConn = DriverManager.getConnection(jdbcUrlSrc, dbUser, dbPass);
                dbSrcOk = true;
                Platform.runLater(() -> {
                    log("Remote DB connected via SSH tunnel.");
                    loadTables();
                });

                // --- 3. Destination DB (local) ---
                String dbHostDest = dotenv.get("DB_DEST_HOST");
                String dbPort = dotenv.get("DB_DEST_PORT");
                String dbUserDest = dotenv.get("DB_DEST_USER");
                String dbPassDest = dotenv.get("DB_DEST_PASS");
                String dbNameDest = dotenv.get("DB_DEST_NAME");
                String jdbcUrlDest = "jdbc:mysql://" + dbHostDest + ":" + dbPort + "/" + dbNameDest
                        + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

                destConn = DriverManager.getConnection(jdbcUrlDest, dbUserDest, dbPassDest);
                dbDestOk = true;
                Platform.runLater(() -> log("Local DB connection successful."));

                // --- 4. Enable sync buttons ---
                Platform.runLater(this::enableSyncIfReady);

            } catch (Exception e) {
                sshOk = dbSrcOk = dbDestOk = false;
                Platform.runLater(() -> {
                    connStatusLabel.setText("Failed");
                    connStatusLabel.setStyle("-fx-text-fill: red;");
                    log("Connection failed: " + e.getMessage());
                    isBusy = false;
                });
            }
        }).start();
    }

    private List<String> getTableList(Connection conn) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES")) {
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        return tables;
    }

    // Load Tables into ComboBox
    private void loadTables() {
        new Thread(() -> {
            try (Statement stmt = srcConn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SHOW TABLES");
                List<String> tables = new ArrayList<>();
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
                Platform.runLater(() -> {
                    tableSelector.getItems().setAll(tables);
                    log("Loaded " + tables.size() + " tables from remote DB.");
                });
            } catch (Exception e) {
                Platform.runLater(() -> log("Failed to load tables: " + e.getMessage()));
                tableSelector.getItems().clear();
                isBusy = false;
            }
        }).start();
    }

    // Sync Table Structure
    @FXML
    private void handleSyncStructure() {
        if (isBusy) {
            log("Still working...");
            return;
        }
        isBusy = true;

        String table = tableSelector.getValue();
        if (table == null || table.isEmpty()) {
            log("Please select a table first.");
            isBusy = false;
            return;
        }

        log("Starting table structure sync for: " + table);

        new Thread(() -> {
            try {
                if (srcConn == null || destConn == null || srcConn.isClosed() || destConn.isClosed()) {
                    Platform.runLater(() -> {
                        log("Connections are not open. Please reconnect first.");
                        isBusy = false;
                    });
                    return;
                }
                Map<String, ColumnInfo> sourceCols = getTableColumns(srcConn, table);
                Map<String, ColumnInfo> destCols = getTableColumns(destConn, table);

                List<String> alterStatements = new ArrayList<>();

                for (var entry : sourceCols.entrySet()) {
                    String col = entry.getKey();
                    ColumnInfo srcInfo = entry.getValue();
                    String colSql = srcInfo.toSQL().replaceAll("\\(\\d+\\)", "");
                    if (!destCols.containsKey(col)) {
                        alterStatements.add("ALTER TABLE " + table + " ADD COLUMN " + colSql + ";");
                    } else {
                        ColumnInfo destInfo = destCols.get(col);
                        if (!srcInfo.equals(destInfo)) {
                            alterStatements.add("ALTER TABLE " + table + " MODIFY COLUMN " + colSql + ";");
                        }
                    }
                }

                for (String col : destCols.keySet()) {
                    if (!sourceCols.containsKey(col)) {
                        alterStatements.add("ALTER TABLE " + table + " DROP COLUMN " + col + ";");
                    }
                }

                if (alterStatements.isEmpty()) {
                    Platform.runLater(() -> log("Structures are already synchronized."));
                } else {
                    for (String sql : alterStatements) {
                        final String query = sql;
                        Platform.runLater(() -> log("Applying: " + query));
                        try (Statement stmt = destConn.createStatement()) {
                            stmt.execute(query);
                        }
                    }
                    Platform.runLater(() -> log("Structure synchronization completed."));
                }

            } catch (SQLException e) {
                Platform.runLater(() -> log("Struct sync failed: " + e.getMessage()));
            } catch (Exception e) {
                Platform.runLater(() -> log("Unexpected error: " + e.getMessage()));
            } finally {
                Platform.runLater(() -> isBusy = false);
            }
        }).start();
    }

    // Sync table data
    @FXML
    private void handleSyncData() {
        if (isBusy){
            log("Still working...");
            return;
        }
        isBusy = true;
        String table = tableSelector.getValue();
        if (table == null || table.isEmpty()) {
            log("Select a table first.");
            isBusy = false;
            return;
        }
        syncTableData(table);
    }

    @FXML
    private void handleSyncFavorites(){
        if (isBusy) {
            log("Still working...");
            return;
        }
        isBusy = true;

        new Thread(() -> {
            try {
                List<String> favorites = new ArrayList<>();
                InputStream inputStream = getClass().getResourceAsStream("/favorites.txt");
                if (inputStream == null) {
                    System.out.println("favorites.txt not found in resources!");
                    return;
                }
                try{
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty() || line.startsWith("#")) continue;
                        favorites.add(line);
                    }
                } catch (IOException e) {
                    Platform.runLater(() -> log("No favorites found or error reading file: " + e.getMessage()));
                    return;
                }

                if (favorites.isEmpty()) {
                    Platform.runLater(() -> log("No favorite tables to sync."));
                    return;
                }

                Platform.runLater(() -> log("Syncing " + favorites.size() + " favorite table(s): " + favorites));

                for (String table : favorites) {
                    final String t = table;
                    Platform.runLater(() -> log("Syncing table: " + t));
                    syncTableData(t);
                }

                Platform.runLater(() -> log("Favorites sync completed."));

            } finally {
                Platform.runLater(() -> isBusy = false);
            }
        }).start();
    }

    public void syncTableData(String table){
        if (table == null || table.isEmpty()) {
            log("Select a table first.");
            isBusy = false;
            return;
        }
        log("Starting batch data synchronization for table: " + table);

        new Thread(() -> {
            try {
                if (srcConn == null || destConn == null || srcConn.isClosed() || destConn.isClosed()) {
                    Platform.runLater(() -> {
                        log("Connections are not open. Please reconnect first.");
                        isBusy = false;
                    });
                    return;
                }
                DatabaseMetaData meta = srcConn.getMetaData();
                ResultSet pkRs = meta.getPrimaryKeys(null, null, table);

                List<String> pkCols = new ArrayList<>();
                while (pkRs.next()) pkCols.add(pkRs.getString("COLUMN_NAME"));

                if (pkCols.isEmpty()) {
                    Platform.runLater(() -> {
                        log("Table " + table + " has no primary key — cannot safely sync.");
                        isBusy = false;
                    });
                    return;
                }

                String pk = pkCols.get(0);
                log("Primary key detected: " + pk);

                try (Statement srcStmt = srcConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                    srcStmt.setFetchSize(500); // for large tables
                    ResultSet srcData = srcStmt.executeQuery("SELECT * FROM " + table);

                    ResultSetMetaData rsMeta = srcData.getMetaData();
                    int colCount = rsMeta.getColumnCount();

                    // Build column list and placeholders
                    StringBuilder colNames = new StringBuilder();
                    StringBuilder placeholders = new StringBuilder();
                    StringBuilder updates = new StringBuilder();

                    for (int i = 1; i <= colCount; i++) {
                        String col = rsMeta.getColumnName(i);
                        colNames.append("`").append(col).append("`");
                        placeholders.append("?");
                        if (!col.equals(pk)) {
                            updates.append("`").append(col).append("`=VALUES(`").append(col).append("`),");
                        }
                        if (i < colCount) {
                            colNames.append(",");
                            placeholders.append(",");
                        }
                    }
                    if (updates.length() > 0) updates.setLength(updates.length() - 1);

                    String sql = "INSERT INTO `" + table + "` (" + colNames + ") VALUES (" + placeholders + ") "
                            + "ON DUPLICATE KEY UPDATE " + updates;

                    try (PreparedStatement ps = destConn.prepareStatement(sql)) {
                        int batchSize = 0;
                        int totalRows = 0;

                        while (srcData.next()) {
                            for (int i = 1; i <= colCount; i++) {
                                Object value = srcData.getObject(i);
                                ps.setObject(i, value);
                            }
                            ps.addBatch();
                            batchSize++;
                            totalRows++;

                            if (batchSize >= 100) {
                                ps.executeBatch();
                                batchSize = 0;
                                int progress = totalRows;
                                Platform.runLater(() -> log("Synced " + progress + " rows so far..."));
                            }
                        }

                        if (batchSize > 0) ps.executeBatch();
                        String msg = "Data synchronization completed for " + totalRows + " rows in table: " + table;
                        Platform.runLater(() -> log(msg));
                    }
                }

            } catch (Exception e) {
                Platform.runLater(() -> log("Data sync failed: " + e.getMessage()));
            } finally {
                Platform.runLater(() -> isBusy = false);
            }
        }).start();
    }

    private void disableActionButtons(boolean status){
        openConnButton.setDisable(!status); // enable when connected
        closeConnButton.setDisable(status); // enable when disconnected
        getFavsButton.setDisable(status);
        getUpdatesButton.setDisable(status);
        syncStructButton.setDisable(status);
        syncDataButton.setDisable(status);
    }

    // Enable sync button if both connections are OK
    private void enableSyncIfReady() {
        if (sshOk && dbSrcOk && dbDestOk) {
            connStatusLabel.setText("Connected");
            connStatusLabel.setStyle("-fx-text-fill: green;");

            disableActionButtons(false);

            log("Both SSH and DB connections are OK. Ready to sync.");

            isBusy = false;
        }
    }

    // Append to log area
    private void log(String msg) {
        Platform.runLater(() -> logArea.appendText("> " + msg + "\n"));
    }

    private Map<String, ColumnInfo> getTableColumns(Connection conn, String tableName) throws SQLException {
        Map<String, ColumnInfo> map = new LinkedHashMap<>();
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getColumns(null, null, tableName, null);

        while (rs.next()) {
            String colName = rs.getString("COLUMN_NAME");
            String type = rs.getString("TYPE_NAME");
            int size = rs.getInt("COLUMN_SIZE");
            String nullable = rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable ? "YES" : "NO";
            String def = rs.getString("COLUMN_DEF");

            map.put(colName, new ColumnInfo(colName, type, size, nullable, def));
        }
        return map;
    }

    // Simple holder for column metadata
    class ColumnInfo {
        String name;
        String type;
        int size;
        String nullable;
        String defaultValue;

        ColumnInfo(String name, String type, int size, String nullable, String defaultValue) {
            this.name = name;
            this.type = type;
            this.size = size;
            this.nullable = nullable;
            this.defaultValue = defaultValue;
        }

        public String toSQL() {
            StringBuilder sb = new StringBuilder(name + " " + type);
            if (size > 0 && !type.equalsIgnoreCase("text") && !type.equalsIgnoreCase("blob")) {
                sb.append("(").append(size).append(")");
            }
            if ("NO".equals(nullable)) sb.append(" NOT NULL");
            if (defaultValue != null) sb.append(" DEFAULT '").append(defaultValue).append("'");
            return sb.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof ColumnInfo)) return false;
            ColumnInfo other = (ColumnInfo) o;
            return Objects.equals(type, other.type)
                    && size == other.size
                    && Objects.equals(nullable, other.nullable)
                    && Objects.equals(defaultValue, other.defaultValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, size, nullable, defaultValue);
        }
    }
}