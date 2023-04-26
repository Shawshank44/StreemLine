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

    insert(databasename,clustername,data,allowDuplicates = false){
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
                  const writeStream = fs.createWriteStream(clusterpath);
                  writeStream.write(JSON.stringify(read));
                  writeStream.end(() => {
                    return resolve();
                  });
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

    Query(databasename, clustername, QUERY_FUNCTION){
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
                const queries = read.filter(QUERY_FUNCTION);
                resolve(queries);
              });
              readstream.on('error', (err) => {
                reject(new Error(`Error reading data: ${err.message}`));
              });
            } catch (error) {
              reject(error);
            }
          });
    }
    update(databasename,clustername,QUERY_FUNCTION,updatedData){
        return new Promise((resolve, reject)=>{
            const clusterpath = path.join(databasename,`${clustername}.json`)
    
            if (!fs.existsSync(clusterpath)){
                return reject(new Error('cluster does not exist in the database'))
            }
    
            const readstream = fs.createReadStream(clusterpath,{encoding:'utf8'})
            let datastr = ''
            readstream.on('data',(chunk)=>{
                datastr += chunk
            })
            readstream.on('end',()=>{
                try {
                    const read = JSON.parse(datastr)
                    read.forEach(row=>{
                        if (QUERY_FUNCTION(row)) {
                            Object.keys(updatedData).forEach(key=>{
                                row[key] = updatedData[key]
                            })
                        }
                    })
                    const writeStream = fs.createWriteStream(clusterpath)
                    writeStream.write(JSON.stringify(read))
                    writeStream.end()
    
                    writeStream.on('finish', ()=>{
                        resolve('data updated successfully')
                    })
    
                    writeStream.on('error',err =>{
                        reject(new Error(`Failed to write to cluster file: ${err.message}`))
                    })
                } catch (err) {
                    reject(new Error(`Failed to parse cluster file: ${err.message}`))
                }
            })
            readstream.on('error',err=>{
                reject(new Error(`Failed to read cluster file: ${err.message}`))
            })
        })

    }

    delete(databaseName, clustername, QUERY_FUNCTION){
        return new Promise((resolve, reject) => {
            const clusterpath = path.join(databaseName, `${clustername}.json`);
            if (!fs.existsSync(clusterpath)) {
              return reject(new Error('Cluster does not exist'));
            }
        
            const readStream = fs.createReadStream(clusterpath);
            let readData = '';
        
            readStream.on('data', chunk => {
              readData += chunk;
            });
        
            readStream.on('end', () => {
              let read;
              try {
                read = JSON.parse(readData);
              } catch (err) {
                return reject(new Error(`Error parsing data: ${err.message}`));
              }
        
              const deleteQuery = read.filter(row => !QUERY_FUNCTION(row));
              read = deleteQuery;
        
              const writeStream = fs.createWriteStream(clusterpath);
              writeStream.write(JSON.stringify(read));
              writeStream.end();
              writeStream.on('finish', () => {
                resolve('Data deleted successfully');
              });
              writeStream.on('error', error => {
                reject(new Error(`Error deleting data: ${error.message}`));
              });
            });
        
            readStream.on('error', error => {
              reject(new Error(`Error reading data: ${error.message}`));
            });
          });
    }

    search(databaseName, clustername, searchElement){
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
                        const values = Object.values(obj);
                        for (let i = 0; i < values.length; i++) {
                            if ((typeof values[i] === "string" || typeof values[i] === "number") && String(values[i]).includes(searchElement)) {
                                return true;
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


}


const db = new Database()

// creating the database
// db.createDatabase('keepers').then(()=>{
//     console.log('database created successfully');
// }).catch((err)=>{
//     console.log(err);
// })

// creating the cluster
// db.createCluster('keepers','agents').then(()=>{
//     console.log('cluster created');
// }).catch((err)=>{
//     console.log(err);
// })

// inserting data into the database
// db.insert('keepers','agents',{name: 'sukesh',age:18,phonenumber:9876543209,gender:'male'},false).then(()=>{
//     console.log('data inserted successfully');
// }).catch((err)=>{
//     console.log(err);
// })

// Query the data cluster:
// db.Query('keepers','agents',(data)=>data)
// .then((data)=>{
//     console.log(data);
// }).catch((err)=>{
//     console.log(err);
// })

// Update the data : 
// db.update('keepers','agents',(data)=>data.name ==='shashank',{gender : 'male'}).then(()=>console.log('data updated')).catch((err)=>console.log(err))

// delete data:
// db.delete('keepers','agents',(data)=>data.age === 18).then(()=>{
//     console.log('data deleted sucessfully');
// }).catch((err)=>{
//     console.log(err);
// })

// db.search('keepers','agents','male').then((data)=>{
//     console.log(data);
// }).catch((err)=>{
//     console.log(err);
// })
