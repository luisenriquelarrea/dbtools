package dbtools;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class App extends Application {
    @Override
    public void start(Stage primaryStage) throws Exception {
        FXMLLoader loader = new FXMLLoader(getClass().getResource("/sync_view.fxml"));
        Scene scene = new Scene(loader.load(), 720, 520);

        // Get the controller instance from FXML
        SyncController controller = loader.getController();

        primaryStage.setTitle("DBTools");
        primaryStage.setScene(scene);
        primaryStage.show();

        primaryStage.setOnCloseRequest(event -> {
            controller.closeConnections();
        });
    }

    public static void main(String[] args) {
        launch(args);
    }
}