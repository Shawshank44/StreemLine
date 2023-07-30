const fs = require('fs')
const path = require('path')


class Database{
    createDatabase(databasename){
        const databasepath = path.join(__dirname, databasename);
            return new Promise((resolve, reject) => {
            try {
                if (fs.existsSync(databasepath)) {
                    throw new Error('Database already exists');
                } else {
                    fs.mkdir(databasepath, (err) => {
                        if (err) {
                            throw err;
                        } else {
                            resolve('Database created successfully');
                        }
                    });
                }
            } catch (error) {
                reject(error);
            }
        });
    }

    createCluster(databasename,clustername){
        const clusterpath = path.join(databasename, `${clustername}.json`);
        return new Promise((resolve, reject) => {
            try {
                if (fs.existsSync(clusterpath)) {
                    throw new Error('Cluster already exists');
                } else {
                    const writeStream = fs.createWriteStream(clusterpath);
                    writeStream.on('finish', () => {
                        resolve('Cluster created successfully');
                    });
                    writeStream.on('error', (err) => {
                        reject({ err, message: 'Could not create the cluster' });
                    });

                    writeStream.write(JSON.stringify([]));
                    writeStream.end();
                }
            } catch (error) {
                reject(error);
            }
        });
    }

    batchWriteData(clusterpath, data) {
        return new Promise((resolve, reject) => {
          const writeStream = fs.createWriteStream(clusterpath);
          writeStream.write(JSON.stringify(data, null, 2));
          writeStream.end(() => {
            resolve();
          });
          writeStream.on('error', (err) => {
            reject(new Error(`Failed to write to cluster file: ${err.message}`));
          });
        });
      }

      insert(databasename, clustername, data, allowDuplicates = false) {
        return new Promise((resolve, reject) => {
          try {
            const clusterpath = path.join(databasename, `${clustername}.json`);
            if (!fs.existsSync(clusterpath)) {
              return reject(new Error('Cluster does not exist in the database'));
            }
            const readstream = fs.createReadStream(clusterpath, { encoding: 'utf8' });
            let datastr = '';
            readstream.on('data', (chunk) => {
              datastr += chunk;
            });
    
            readstream.on('end', () => {
              const read = JSON.parse(datastr);
              const isduplicate = read.some((item) => {
                return JSON.stringify(item) === JSON.stringify(data);
              });
    
              if (!isduplicate || allowDuplicates) {
                read.push(data);
                this.batchWriteData(clusterpath, read)
                  .then(() => resolve())
                  .catch((err) => reject(new Error(`Failed to write to cluster file: ${err.message}`)));
              } else {
                return resolve();
              }
            });
    
            readstream.on('error', (err) => {
              return reject(new Error(`Failed to read cluster file: ${err.message}`));
            });
          } catch (error) {
            reject(error);
          }
        });
      }


      Query(databasename, clustername, QUERY_FUNCTION, options = {}) {
        const clusterpath = path.join(databasename, `${clustername}.json`);
        if (!fs.existsSync(clusterpath)) {
          return Promise.reject(new Error('Cluster does not exist in the database'));
        }
    
        return new Promise((resolve, reject) => {
          const readstream = fs.createReadStream(clusterpath, { encoding: 'utf8' });
          let datastr = '';
          readstream.on('data', (chunk) => {
            datastr += chunk;
          });
          readstream.on('end', () => {
            const read = JSON.parse(datastr);
            let queries = read.filter(QUERY_FUNCTION);
    
            // Aggregation: Calculate the sum of a numeric field
            if (options.aggregate === 'sum' && options.aggregateField) {
              const sum = queries.reduce((acc, obj) => acc + obj[options.aggregateField], 0);
              resolve(sum);
              return;
            }
    
            // Aggregation: Calculate the average of a numeric field
            if (options.aggregate === 'average' && options.aggregateField) {
              const sum = queries.reduce((acc, obj) => acc + obj[options.aggregateField], 0);
              const average = sum / queries.length;
              resolve(average);
              return;
            }
    
            // Sorting
            if (options.sortBy) {
              queries.sort((a, b) => {
                if (options.sortOrder === 'desc') {
                  return b[options.sortBy].localeCompare(a[options.sortBy]);
                }
                return a[options.sortBy].localeCompare(b[options.sortBy]);
              });
            }
    
            // Pagination
            if (options.page && options.pageSize) {
              const startIndex = (options.page - 1) * options.pageSize;
              const endIndex = startIndex + options.pageSize;
              queries = queries.slice(startIndex, endIndex);
            }
    
            // Query Projection
            if (options.fields) {
              queries = queries.map((obj) => {
                const projectedObj = {};
                options.fields.forEach((field) => {
                  projectedObj[field] = obj[field];
                });
                return projectedObj;
              });
            }
    
            // Limit number of results
            if (options.limit) {
              queries = queries.slice(0, options.limit);
            }
    
            resolve(queries);
          });
          readstream.on('error', (err) => {
            reject(new Error(`Error reading data: ${err.message}`));
          });
        });
      }

    
    update(databasename, clustername, QUERY_FUNCTION, updatedData) {
        return new Promise((resolve, reject) => {
          const clusterpath = path.join(databasename, `${clustername}.json`);
    
          if (!fs.existsSync(clusterpath)) {
            return reject(new Error('cluster does not exist in the database'));
          }
    
          const readstream = fs.createReadStream(clusterpath, { encoding: 'utf8' });
          let datastr = '';
          readstream.on('data', (chunk) => {
            datastr += chunk;
          });
          readstream.on('end', () => {
            try {
              const read = JSON.parse(datastr);
              read.forEach((row) => {
                if (QUERY_FUNCTION(row)) {
                  Object.keys(updatedData).forEach((key) => {
                    row[key] = updatedData[key];
                  });
                }
              });
    
              this.batchWriteData(clusterpath, read)
                .then(() => resolve('data updated successfully'))
                .catch((err) => reject(new Error(`Failed to write to cluster file: ${err.message}`)));
            } catch (err) {
              reject(new Error(`Failed to parse cluster file: ${err.message}`));
            }
          });
          readstream.on('error', (err) => {
            reject(new Error(`Failed to read cluster file: ${err.message}`));
          });
        });
      }

      delete(databaseName, clustername, QUERY_FUNCTION) {
        return new Promise((resolve, reject) => {
          const clusterpath = path.join(databaseName, `${clustername}.json`);
          if (!fs.existsSync(clusterpath)) {
            return reject(new Error('Cluster does not exist'));
          }
    
          const readStream = fs.createReadStream(clusterpath);
          let readData = '';
    
          readStream.on('data', (chunk) => {
            readData += chunk;
          });
    
          readStream.on('end', () => {
            try {
              let read = JSON.parse(readData);
    
              // Filter out the data that meets the deletion criteria
              const deleteQuery = read.filter((row) => !QUERY_FUNCTION(row));
    
              // Find the IDs of the deleted data
              const deletedIDs = read.filter((row) => QUERY_FUNCTION(row)).map((row) => row.id);
    
              // Remove any references to the deleted IDs from the many relationships
              deleteQuery.forEach((row) => {
                if (row.manyToManyRelationship) {
                  row.manyToManyRelationship = row.manyToManyRelationship.filter((id) => !deletedIDs.includes(id));
                }
              });
    
              read = deleteQuery;
    
              // Perform batch write to update the cluster data
              this.batchWriteData(clusterpath, read)
                .then(() => resolve('Data deleted successfully'))
                .catch((err) => reject(new Error(`Failed to write to cluster file: ${err.message}`)));
            } catch (err) {
              reject(new Error( ));
            }
          });
    
          readStream.on('error', (error) => {
            reject(new Error(`Error reading data: ${error.message}`));
          });
        });
      }
    

      search(databaseName, clustername, searchElement, searchFields, caseInsensitive = true, useRegex = false) {
        return new Promise((resolve, reject) => {
          const clusterpath = path.join(databaseName, `${clustername}.json`);
          if (!fs.existsSync(clusterpath)) {
            return reject(new Error('Cluster does not exist'));
          }
      
          const readStream = fs.createReadStream(clusterpath);
          readStream.on('error', error => reject(new Error(`Error reading data: ${error.message}`)));
      
          let readData = '';
          readStream.on('data', chunk => {
            readData += chunk;
          });
      
          readStream.on('end', () => {
            try {
              let read = JSON.parse(readData);
              const results = read.filter(obj => {
                for (const field of searchFields) {
                  const value = obj[field];
                  if (value) {
                    let strToMatch = String(value);
                    if (caseInsensitive) {
                      strToMatch = strToMatch.toLowerCase();
                      searchElement = searchElement.toLowerCase();
                    }
                    if (useRegex) {
                      const regex = new RegExp(searchElement);
                      if (regex.test(strToMatch)) {
                        return true;
                      }
                    } else {
                      if (strToMatch.includes(searchElement)) {
                        return true;
                      }
                    }
                  }
                }
                return false;
              });
              resolve(results);
            } catch (error) {
              reject(new Error(`Error parsing JSON data: ${error.message}`));
            }
          });
        });
      }
    


    
    createLink(databaseName, clusterName, sourceIDs, targetIDs) {
        return new Promise((resolve, reject) => {
          try {
            const clusterPath = path.join(databaseName, `${clusterName}.json`);
      
            if (!fs.existsSync(clusterPath)) {
              throw new Error('Cluster does not exist in the database');
            }
      
            let clusterData = JSON.parse(fs.readFileSync(clusterPath, { encoding: 'utf8' }));
      
            // Function to find a node by its ID in the cluster data
            const findNodeById = (id) => clusterData.find((node) => node.id === id);
      
            sourceIDs.forEach((sourceID) => {
              // Find the source node in the cluster data
              const sourceNode = findNodeById(sourceID);
      
              if (!sourceNode) {
                throw new Error(`Source node with ID ${sourceID} not found`);
              }
      
              // Add the many-to-many relationship property to the source node
              if (!sourceNode.manyToManyRelationship) {
                sourceNode.manyToManyRelationship = [];
              }
      
              // Add the target IDs to the many-to-many relationship array of the source node
              targetIDs.forEach((targetID) => {
                if (!sourceNode.manyToManyRelationship.includes(targetID)) {
                  sourceNode.manyToManyRelationship.push(targetID);
      
                  // Find the target node in the cluster data
                  const targetNode = findNodeById(targetID);
      
                  if (targetNode) {
                    // Add the many-to-many relationship property to the target node
                    if (!targetNode.manyToManyRelationship) {
                      targetNode.manyToManyRelationship = [];
                    }
      
                    // Add the sourceID to the many-to-many relationship array of the target node
                    if (!targetNode.manyToManyRelationship.includes(sourceID)) {
                      targetNode.manyToManyRelationship.push(sourceID);
                    }
                  }
                }
              });
            });
      
            // Perform batch write to update the cluster data
            this.batchWriteData(clusterPath, clusterData)
              .then(() => resolve('Many-to-many relationships created successfully'))
              .catch((err) => reject(new Error(`Failed to write to cluster file: ${err.message}`)));
          } catch (error) {
            reject(error);
          }
        });
    }
    

}


const db = new Database()

// creating the database
// db.createDatabase('users').then(()=>{
//     console.log('database created successfully');
// }).catch((err)=>{
//     console.log(err);
// })

// creating the cluster
// db.createCluster('users','agents').then(()=>{
//     console.log('cluster created');
// }).catch((err)=>{
//     console.log(err);
// })

// inserting data into the database
// db.insert('users','agents',{id:'2009', name : 'jehul',age : 27,phonenumber:987654204},false).then(()=>{
//     console.log('data inserted successfully');
// }).catch((err)=>{
//     console.log(err);
// })

// db.createLink('users', 'agents', ['2004'], ['2006'])
//   .then(() => {
//     console.log('many-to-many relationship created successfully');
//   })
//   .catch((error) => {
//     console.error('Error creating one-to-many relationship:', error);
//   });


// Query the data cluster:
// db.Query('users','agents',(data)=>data.age  < 25,{sortBy : 'name'})
// .then((data)=>{
//     console.log(data);
// }).catch((err)=>{
//     console.log(err);
// })

// Update the data : 
// db.update('users','agents',(data)=>data.name === 'shashank',{age : 22}).then(()=>console.log('data updated')).catch((err)=>console.log(err))

// delete data:
// db.delete('users', 'agents', (data) => data.name === 'sehul')
//   .then(() => {
//       console.log('deleted success');
//   })
//   .catch((err) => {
//     console.log(err);
//   });

// db.search('users', 'agents', '2004', ['id','name','age'], true, true)
//   .then(data => {
//     console.log(data);
//   })
//   .catch(err => {
//     console.log(err);
//   });

