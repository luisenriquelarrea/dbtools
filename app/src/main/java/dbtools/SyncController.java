package dbtools;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import io.github.cdimascio.dotenv.Dotenv;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.*;

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

    @FXML private Button checkSSHButton;
    @FXML private Label sshStatusLabel;
    @FXML private Button checkDBButton;
    @FXML private Label dbStatusLabel;
    @FXML private ComboBox<String> tableSelector;
    @FXML private Button syncStructButton;
    @FXML private Button syncDataButton;
    @FXML private TextArea logArea;

    private final Dotenv dotenv = Dotenv.load();
    private boolean sshOk = false;
    private boolean dbSrcOk = false;
    private boolean dbDestOk = false;
    private Session sshSession;
    
    private Connection srcConn;
    private Connection destConn;

    // Called automatically after FXML loads
    @FXML
    public void initialize() {
        log("Ready. Please validate SSH and DB connections.");
    }

    public void closeConnections() {
        try {
            if (srcConn != null && !srcConn.isClosed()) {
                srcConn.close();
                log("✅ Source DB connection closed");
            }
        } catch (Exception e) {
            log("⚠️ Error closing source DB: " + e.getMessage());
        }
    
        try {
            if (destConn != null && !destConn.isClosed()) {
                destConn.close();
                log("✅ Destination DB connection closed");
            }
        } catch (Exception e) {
            log("⚠️ Error closing destination DB: " + e.getMessage());
        }
    
        try {
            if (sshSession != null && sshSession.isConnected()) {
                sshSession.disconnect();
                log("🔒 SSH session disconnected");
            }
        } catch (Exception e) {
            log("⚠️ Error closing SSH session: " + e.getMessage());
        }
    
        Platform.runLater(() -> {
            sshStatusLabel.setText("🔌 Disconnected");
            sshStatusLabel.setStyle("-fx-text-fill: gray;");
            //dbSrcStatusLabel.setText("❌ Disconnected");
            //dbDestStatusLabel.setText("❌ Disconnected");
        });
    }    

    // 🔐 SSH Connection Check
    @FXML
    private void openSSH() {
        new Thread(() -> {
            try {
                String sshHost = dotenv.get("SSH_HOST");
                String sshUser = dotenv.get("SSH_USER");
                String sshPass = dotenv.get("SSH_PASS");
                int sshPort = Integer.parseInt(dotenv.get("SSH_PORT", "22"));
                int localPort = 3307;  // pick an available port on your computer
                int remotePort = 3306; // MySQL port on the remote server
                String remoteHost = "127.0.0.1"; // from SSH server’s perspective

                JSch jsch = new JSch();
                sshSession = jsch.getSession(sshUser, sshHost, sshPort);
                sshSession.setPassword(sshPass);
                sshSession.setConfig("StrictHostKeyChecking", "no");
                sshSession.connect(5000);
                // Create SSH tunnel
                sshSession.setPortForwardingL(localPort, remoteHost, remotePort);
                sshOk = sshSession.isConnected();
                Platform.runLater(() -> {
                    sshStatusLabel.setText("✅ Connected");
                    sshStatusLabel.setStyle("-fx-text-fill: green;");
                    log("SSH connection successful.");
                    enableSyncIfReady();
                });
            } catch (Exception e) {
                sshOk = false;
                Platform.runLater(() -> {
                    sshStatusLabel.setText("❌ Failed");
                    sshStatusLabel.setStyle("-fx-text-fill: red;");
                    log("SSH connection failed: " + e.getMessage());
                });
            }
        }).start();
    }

    private void openDestDb() {
        new Thread(() -> {
            try {
                String dbHost = dotenv.get("DB_DEST_HOST");
                String dbPort = dotenv.get("DB_DEST_PORT");
                String dbUser = dotenv.get("DB_DEST_USER");
                String dbPass = dotenv.get("DB_DEST_PASS");
                String dbName = dotenv.get("DB_DEST_NAME");

                String jdbcUrl = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName
                        + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

                destConn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);

                dbDestOk = !destConn.isClosed();
                Platform.runLater(() -> {
                    dbStatusLabel.setText("✅ Connected");
                    dbStatusLabel.setStyle("-fx-text-fill: green;");
                    log("Database localhost connection successful.");
                    enableSyncIfReady();
                });

            } catch (Exception e) {
                dbDestOk = false;
                Platform.runLater(() -> {
                    dbStatusLabel.setText("❌ Failed");
                    dbStatusLabel.setStyle("-fx-text-fill: red;");
                    log("Database localhost connection failed: " + e.getMessage());
                });
            }
        }).start();
    }

    // 🗄️ Database Connection Check
    @FXML
    private void openSourceDb() {
        new Thread(() -> {
            try {
                String dbHost = dotenv.get("DB_SRC_HOST");
                String dbPort = dotenv.get("DB_SRC_PORT");
                String dbUser = dotenv.get("DB_SRC_USER");
                String dbPass = dotenv.get("DB_SRC_PASS");
                String dbName = dotenv.get("DB_SRC_NAME");

                String jdbcUrl = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName
                        + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";

                srcConn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);

                dbSrcOk = !srcConn.isClosed();
                Platform.runLater(() -> {
                    //dbStatusLabel.setText("✅ Connected");
                    //dbStatusLabel.setStyle("-fx-text-fill: green;");
                    log("Database remote connection successful.");
                    //enableSyncIfReady();
                    loadTables();
                    openDestDb();
                });

            } catch (Exception e) {
                dbSrcOk = false;
                Platform.runLater(() -> {
                    dbStatusLabel.setText("❌ Failed");
                    dbStatusLabel.setStyle("-fx-text-fill: red;");
                    log("Database remote connection failed: " + e.getMessage());
                });
            }
        }).start();
    }

    // 📋 Load Tables into ComboBox
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
            }
        }).start();
    }

    // 🔄 Sync Table Structure
    @FXML
    private void handleSyncStructure() {
        String table = tableSelector.getValue();
        if (table == null || table.isEmpty()) {
            log("⚠️ Please select a table first.");
            return;
        }
        log("Starting table structure sync for: " + table);

        new Thread(() -> {
            try{
                Map<String, ColumnInfo> sourceCols = getTableColumns(srcConn, table);
                Map<String, ColumnInfo> destCols = getTableColumns(destConn, table);

                List<String> alterStatements = new ArrayList<>();

                // 1️⃣ Find missing or mismatched columns
                for (var entry : sourceCols.entrySet()) {
                    String col = entry.getKey();
                    ColumnInfo srcInfo = entry.getValue();

                    if (!destCols.containsKey(col)) {
                        // Missing column
                        alterStatements.add("ALTER TABLE " + table + " ADD COLUMN " + srcInfo.toSQL() + ";");
                    } else {
                        ColumnInfo destInfo = destCols.get(col);
                        if (!srcInfo.equals(destInfo)) {
                            // Type, nullability, or default mismatch
                            alterStatements.add("ALTER TABLE " + table + " MODIFY COLUMN " + srcInfo.toSQL() + ";");
                        }
                    }
                }

                // 2️⃣ Find columns in destination but not in source
                for (String col : destCols.keySet()) {
                    if (!sourceCols.containsKey(col)) {
                        alterStatements.add("ALTER TABLE " + table + " DROP COLUMN " + col + ";");
                    }
                }

                // 3️⃣ Apply the changes
                if (alterStatements.isEmpty()) {
                    Platform.runLater(() -> log("✅ Structures are already synchronized."));
                } else {
                    for (String sql : alterStatements) {
                        Platform.runLater(() -> log("Applying: " + sql));
                        try (Statement stmt = destConn.createStatement()) {
                            stmt.execute(sql);
                        }
                    }
                    Platform.runLater(() -> log("✅ Structure synchronization completed."));
                }
            } catch (SQLException e) {
                Platform.runLater(() -> log("Struct sync failed: " + e.getMessage()));
            }
        }).start();
    }

    // 💾 Sync table data
    @FXML
    private void handleSyncData() {
        String table = tableSelector.getValue();
        if (table == null || table.isEmpty()) {
            log("⚠️ Select a table first.");
            return;
        }

        log("🔄 Starting batch data synchronization for table: " + table);

        new Thread(() -> {
            try {
                DatabaseMetaData meta = srcConn.getMetaData();
                ResultSet pkRs = meta.getPrimaryKeys(null, null, table);

                List<String> pkCols = new ArrayList<>();
                while (pkRs.next()) pkCols.add(pkRs.getString("COLUMN_NAME"));

                if (pkCols.isEmpty()) {
                    Platform.runLater(() -> log("⚠️ Table " + table + " has no primary key — cannot safely sync."));
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
                                Platform.runLater(() -> log("✅ Synced " + progress + " rows so far..."));
                            }
                        }

                        if (batchSize > 0) ps.executeBatch();
                        String msg = "🎯 Data synchronization completed for " + totalRows + " rows in table: " + table;
                        Platform.runLater(() -> log(msg));
                    }
                }

            } catch (Exception e) {
                Platform.runLater(() -> log("❌ Data sync failed: " + e.getMessage()));
            }
        }).start();
    }

    // ✅ Enable sync button if both connections are OK
    private void enableSyncIfReady() {
        if (sshOk && dbSrcOk && dbDestOk) {
            syncStructButton.setDisable(false);
            syncDataButton.setDisable(false);
            log("Both SSH and DB connections are OK. Ready to sync.");
        }
    }

    // 🪵 Append to log area
    private void log(String msg) {
        Platform.runLater(() -> logArea.appendText(msg + "\n"));
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