const fs = require('fs')
const path = require('path')
const { Transform } = require('stream');


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
            const writeStream = fs.createWriteStream(clusterpath, { encoding: 'utf8' });
    
            writeStream.on('finish', () => {
                resolve();
            });
    
            writeStream.on('error', (err) => {
                reject(new Error(`Failed to write to cluster file: ${err.message}`));
            });
    
            const transformStream = new Transform({
                transform(chunk, encoding, callback) {
                    // Process the chunk (in this case, just pass it along)
                    callback(null, chunk);
                }
            });
    
            transformStream.pipe(writeStream);
    
            // Write each chunk of data to the transform stream
            const chunkSize = 1024 * 1024; // Set your desired chunk size
            for (let i = 0; i < data.length; i += chunkSize) {
                const chunk = data.slice(i, i + chunkSize);
                transformStream.write(JSON.stringify(chunk, null, 2));
            }
    
            transformStream.end();
        });
    }

    batchReadData(clusterpath) {
        return new Promise((resolve, reject) => {
            const readstream = fs.createReadStream(clusterpath, { encoding: 'utf8', highWaterMark: 64 * 1024 }); // Set an initial buffer size
    
            const transformStream = new Transform({
                highWaterMark: readstream._readableState.highWaterMark, // Set the same highWaterMark as the read stream
                transform(chunk, encoding, callback) {
                    // Process the chunk (in this case, append it to the datastr)
                    this.datastr += chunk;
                    callback();
                },
                flush(callback) {
                    try {
                        const read = JSON.parse(this.datastr);
                        resolve(read);
                    } catch (error) {
                        reject(new Error(`Failed to parse cluster file: ${error.message}`));
                    }
                    callback();
                }
            });
    
            transformStream.datastr = ''; // Initialize datastr property
    
            readstream.pipe(transformStream); // Pipe the readstream to the transform stream
    
            readstream.on('error', (err) => {
                reject(new Error(`Failed to read cluster file: ${err.message}`));
            });
        });
    }
    

    insert(databasename, clustername, data, allowDuplicates = false, uniqueFields = []) {
      return new Promise((resolve, reject) => {
          try {
              const clusterpath = path.join(databasename, `${clustername}.json`);
              if (!fs.existsSync(clusterpath)) {
                  return reject(new Error('Cluster does not exist in the database'));
              }
  
              this.batchReadData(clusterpath)
                  .then(read => {
                      const isDuplicate = read.some(item => {
                          return uniqueFields.every(field => item[field] === data[field]);
                      });
  
                      if (isDuplicate) {
                          if (allowDuplicates) {
                              data.duplicate = true;
                          } else {
                              return reject(new Error('Duplicate data found'));
                          }
                      }
  
                      read.push(data);
                      this.batchWriteData(clusterpath, read)
                          .then(() => resolve())
                          .catch(err => reject(new Error(`Failed to write to cluster file: ${err.message}`)));
                  })
                  .catch(err => reject(new Error(`Failed to read cluster file: ${err.message}`)));
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
        this.batchReadData(clusterpath)
            .then(read => {
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
                    queries = queries.map(obj => {
                        const projectedObj = {};
                        options.fields.forEach(field => {
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
            })
            .catch(err => reject(new Error(`Error reading data: ${err.message}`)));
    });
}

update(databasename, clustername, QUERY_FUNCTION, updatedData) {
  return new Promise((resolve, reject) => {
      const clusterpath = path.join(databasename, `${clustername}.json`);

      if (!fs.existsSync(clusterpath)) {
          return reject(new Error('Cluster does not exist in the database'));
      }

      this.batchReadData(clusterpath)
          .then(read => {
              read.forEach(row => {
                  if (QUERY_FUNCTION(row)) {
                      Object.keys(updatedData).forEach(key => {
                          row[key] = updatedData[key];
                      });
                  }
              });
              this.batchWriteData(clusterpath, read)
                  .then(() => resolve('Data updated successfully'))
                  .catch(err => reject(new Error(`Failed to write to cluster file: ${err.message}`)));
          })
          .catch(err => reject(new Error(`Failed to read cluster file: ${err.message}`)));
   });
  }

  updateAllClusters(databaseName, QUERY_FUNCTION, updatedData) {
    return new Promise((resolve, reject) => {
        try {
            const dbPath = path.join(__dirname, databaseName);
            if (!fs.existsSync(dbPath)) {
                return reject(new Error('Database does not exist'));
            }

            fs.readdir(dbPath, (err, files) => {
                if (err) {
                    return reject(new Error(`Error reading database directory: ${err.message}`));
                }

                const updatePromises = files.map(file => {
                    if (file.endsWith('.json')) {
                        const clusterPath = path.join(dbPath, file);
                        return this.batchReadData(clusterPath)
                            .then(read => {
                                read.forEach(row => {
                                    if (QUERY_FUNCTION(row)) {
                                        Object.keys(updatedData).forEach(key => {
                                            row[key] = updatedData[key];
                                        });
                                    }
                                });
                                return this.batchWriteData(clusterPath, read);
                            })
                            .catch(err => {
                                throw new Error(`Failed to update cluster ${file}: ${err.message}`);
                            });
                    }
                });

                Promise.all(updatePromises)
                    .then(() => resolve('Data updated successfully in all clusters'))
                    .catch(error => reject(error));
            });
        } catch (error) {
            reject(error);
        }
    });
}




  delete(databaseName, clustername, QUERY_FUNCTION) {
    return new Promise((resolve, reject) => {
        const clusterpath = path.join(databaseName, `${clustername}.json`);
        if (!fs.existsSync(clusterpath)) {
            return reject(new Error('Cluster does not exist'));
        }

        this.batchReadData(clusterpath)
            .then(read => {
                const deleteQuery = read.filter(row => !QUERY_FUNCTION(row));
                const deletedIDs = read.filter(row => QUERY_FUNCTION(row)).map(row => row.id);

                deleteQuery.forEach(row => {
                    if (row.manyToManyRelationship) {
                        row.manyToManyRelationship = row.manyToManyRelationship.filter(id => !deletedIDs.includes(id));
                    }
                });
                read = deleteQuery;

                this.batchWriteData(clusterpath, read)
                    .then(() => resolve('Data deleted successfully'))
                    .catch(err => reject(new Error(`Failed to write to cluster file: ${err.message}`)));
            })
            .catch(error => reject(new Error(`Error reading data: ${error.message}`)));
    });
}


deleteFromAllClusters(databaseName, QUERY_FUNCTION) {
    return new Promise((resolve, reject) => {
        try {
            const dbPath = path.join(__dirname, databaseName);
            if (!fs.existsSync(dbPath)) {
                return reject(new Error('Database does not exist'));
            }

            fs.readdir(dbPath, (err, files) => {
                if (err) {
                    return reject(new Error(`Error reading database directory: ${err.message}`));
                }

                const deletePromises = files.map(file => {
                    if (file.endsWith('.json')) {
                        const clusterPath = path.join(dbPath, file);
                        return this.batchReadData(clusterPath)
                            .then(read => {
                                const deleteQuery = read.filter(row => !QUERY_FUNCTION(row));
                                return this.batchWriteData(clusterPath, deleteQuery);
                            })
                            .catch(err => {
                                throw new Error(`Failed to delete data from cluster ${file}: ${err.message}`);
                            });
                    }
                });

                Promise.all(deletePromises)
                    .then(() => resolve('Data deleted successfully from all clusters'))
                    .catch(error => reject(error));
            });
        } catch (error) {
            reject(error);
        }
    });
}
    

search(databaseName, clustername, searchElement, searchFields, caseInsensitive = true, useRegex = false) {
  return new Promise((resolve, reject) => {
      const clusterpath = path.join(databaseName, `${clustername}.json`);
      if (!fs.existsSync(clusterpath)) {
          return reject(new Error('Cluster does not exist'));
      }

      this.batchReadData(clusterpath)
          .then(read => {
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
          })
          .catch(error => reject(new Error(`Error reading data: ${error.message}`)));
  });
}

searchAllClusters(databaseName, searchElement, searchFields, caseInsensitive = true, useRegex = false) {
    return new Promise((resolve, reject) => {
        const databasePath = path.join(__dirname, databaseName);
        const clusterFiles = fs.readdirSync(databasePath).filter(file => file.endsWith('.json'));

        const results = [];

        const promises = clusterFiles.map(clusterFile => {
            const clusterpath = path.join(databaseName, clusterFile);
            return this.batchReadData(clusterpath)
                .then(clusterData => {
                    const clusterResults = clusterData.filter(obj => {
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
                    results.push(...clusterResults);
                })
                .catch(error => console.log(`Error reading data from cluster ${clusterFile}: ${error.message}`));
        });

        Promise.all(promises)
            .then(() => {
                resolve(results);
            })
            .catch(error => reject(new Error(`Error searching clusters: ${error.message}`)));
    });
}


    
createLink(databaseName, clusterName, sourceIDs, targetIDs) {
  return new Promise((resolve, reject) => {
      const clusterPath = path.join(databaseName, `${clusterName}.json`);

      if (!fs.existsSync(clusterPath)) {
          return reject(new Error('Cluster does not exist in the database'));
      }

      this.batchReadData(clusterPath)
          .then(clusterData => {
              // Function to find a node by its ID in the cluster data
              const findNodeById = id => clusterData.find(node => node.id === id);

              sourceIDs.forEach(sourceID => {
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
                  targetIDs.forEach(targetID => {
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

              this.batchWriteData(clusterPath, clusterData)
                  .then(() => resolve('Many-to-many relationships created successfully'))
                  .catch(err => reject(new Error(`Failed to write to cluster file: ${err.message}`)));
          })
          .catch(error => reject(new Error(`Error reading data: ${error.message}`)));
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

// db.createCluster('users','orders').then(()=>{
//     console.log('cluster created');
// }).catch((err)=>{
//     console.log(err);
// })


// inserting data into the database
// db.insert('users', 'agents', { id: '2010', name: 'ronny', age: 33, phonenumber: 9876544292},false,   ['id'])
//   .then(() => {
//     console.log('data inserted successfully');
//   })
//   .catch((err) => {
//     console.log(err);
//   });

// db.insert('users', 'orders', { id: '2010',orderid : 407, customer: 'ronny', phonenumber: 9876544292,items : ['spec','shunts','resistors','capacitors'],total : 750},false,   ['orderid'])
//   .then(() => {
//     console.log('data inserted successfully');
//   })
//   .catch((err) => {
//     console.log(err);
//   });


// db.createLink('users', 'agents', ['2004'], ['2001','2008','2006','2010'])
//   .then(() => {
//     console.log('many-to-many relationship created successfully');
//   })
//   .catch((error) => {
//     console.error('Error creating one-to-many relationship:', error);
//   });


// Query the data cluster:
// db.Query('users','agents',(data)=>data.name === 'shashank',{sortBy : 'name'})
// .then((data)=>{
//     console.log(data);
// }).catch((err)=>{
//     console.log(err);
// })

// db.Query('users','orders',(data)=>data.id === '2010')
// .then((data)=>{
//     console.log(data);
// }).catch((err)=>{
//     console.log(err);
// })


// Update the data : 
// db.update('users','agents',(data)=>data.name === 'shashank',{age : 21}).then(()=>console.log('data updated')).catch((err)=>console.log(err))

// db.updateAllClusters('users', (data) => data.id === '2010', {phonenumber: 9876544238})
//   .then(() => {
//     console.log('Data updated in all clusters');
//   })
//   .catch((err) => {
//     console.log(err);
//   });

// delete data:
// db.delete('users', 'agents', (data) => data.customer === 'ronny')
//   .then(() => {
//       console.log('deleted success');
//   })
//   .catch((err) => {
//     console.log(err);
//   });


// db.deleteFromAllClusters('users', (data) => data.id === '2010')
//   .then(() => {
//     console.log('Data deleted from all clusters');
//   })
//   .catch((err) => {
//     console.log(err);
//   });


// db.search('users', 'agents', 'shashank', ['id','name','age'], true, true)
//   .then(data => {
//     console.log(data);
//   })
//   .catch(err => {
//     console.log(err);
//   });

// db.searchAllClusters('users', '2010', ['id','name','age'], true, true)
//   .then(data => {
//     console.log(data);
//   })
//   .catch(err => {
//     console.log(err);
//   });

