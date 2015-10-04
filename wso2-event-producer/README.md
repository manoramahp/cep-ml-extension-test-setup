1. Install ML features and CEP ML Extension feature in WSO2 CEP
   https://docs.wso2.com/display/ML100/WSO2+CEP+Extension+for+ML+Predictions#WSO2CEPExtensionforMLPredictions-InstallingrequiredfeaturesinWSO2CEP

2. Copy the event publisher, event receiver, event stream, execution plan definitions
   inside artifacts folder to relevant folders inside CEP_HOME/repository/deployment/server

3. Unzip the test-dataset-files inside resources/events/ folder

4. Provide the following values in  Client.java
   cepHome
   noOfFeatures : 10, 100, 200, 300, 400, 500, 600, 700

5. Run Client


========================================================================
Events are generated using the test data files inside resources/events
