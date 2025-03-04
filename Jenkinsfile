pipeline {
    agent any
    environment {
        SCRIPT_PATH = "tfl_buses.py"  // Your Python script for buses
        REMOTE_HDFS_FILE = "/tmp/big_datajan2025/TFL/Buses/tfl_realtime_data_buses.csv"
        LOCAL_TEMP_FILE = "merged_tfl_buses.csv"
        TEMP_NEW_FILE = "tfl_realtime_data_buses.csv"  // Generated CSV file from script
    }
    stages {
        stage('Collect Bus Data & Upload to HDFS') {
            steps {
                script {
                    def fileExists = sh(script: "hdfs dfs -test -e ${REMOTE_HDFS_FILE} && echo 'EXIST' || echo 'MISSING'", returnStdout: true).trim()

                    if (fileExists == "EXIST") {
                        echo "✅ HDFS file exists. Merging bus data..."
                        
                        // Step 1: Download existing HDFS data
                        sh "hdfs dfs -cat ${REMOTE_HDFS_FILE} > ${LOCAL_TEMP_FILE}"

                        // Step 2: Run Python script to generate new data
                        sh "python3 ${SCRIPT_PATH}"

                        // Step 3: Remove header from new file (avoiding duplicate headers)
                        sh "tail -n +2 ${TEMP_NEW_FILE} >> ${LOCAL_TEMP_FILE}"

                        // Step 4: Upload merged file back to HDFS
                        sh "hdfs dfs -put -f ${LOCAL_TEMP_FILE} ${REMOTE_HDFS_FILE}"

                        echo "✅ Bus data merged and uploaded to HDFS successfully!"

                    } else {
                        echo "⚠️ HDFS file does not exist. Creating new file..."
                        
                        // First-time upload
                        sh "python3 ${SCRIPT_PATH} && hdfs dfs -put -f ${TEMP_NEW_FILE} ${REMOTE_HDFS_FILE}"

                        echo "✅ New bus data file created and uploaded to HDFS!"
                    }
                }
            }
        }
    }
    triggers {
        cron('H/30 * * * *')  // Runs every 30 minutes
    }
}
