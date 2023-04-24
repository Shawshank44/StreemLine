const fs = require('fs')
const path = require('path')


class Database{
    createDatabase(databasename){
        const databasepath = path.join(__dirname,databasename);
        return new Promise((resolve,reject)=>{
            if(fs.existsSync(databasepath)){
                reject(new Error('database already exists'))
            }else{
                fs.mkdir(databasepath,(err)=>{
                    if (err) {
                        reject(err)
                    }else{
                        resolve('database created successfully')
                    }
                })
            }
        })
    }

    createCluster(databasename,clustername){
        const clusterpath = path.join(databasename,`${clustername}.json`)
        return new Promise((resolve,reject)=>{
            if(fs.existsSync(clusterpath)){
                reject(new Error('cluster already exists'))
            }else{
                const writeStream = fs.createWriteStream(clusterpath)
                writeStream.on('finish',()=>{
                    resolve('cluster created successfully')
                })
                writeStream.on('error',(err)=>{
                    reject({err,message:'could not create the cluster'})
                })
                writeStream.write(JSON.stringify([]))
                writeStream.end();
            }
        })
    }

    insert(databasename,clustername,data,allowDuplicates = false){
        return new Promise((resolve,reject)=>{
            const clusterpath = path.join(databasename,`${clustername}.json`)
            if (!fs.existsSync(clusterpath)){
                return reject(new Error('cluster does not exists in the database'))
            }
            const readstream = fs.createReadStream(clusterpath,{encoding:'utf8'})
            let datastr = ''
            readstream.on('data',(chunk)=>{
                datastr += chunk
            })

            readstream.on('end',()=>{
                const read = JSON.parse(datastr)
                const isduplicate = read.some((item)=>{
                    return JSON.stringify(item) === JSON.stringify(data)
                })

                if (!isduplicate || allowDuplicates) {
                    read.push(data)
                    const writeStream = fs.createWriteStream(clusterpath)
                    writeStream.write(JSON.stringify(read))
                    writeStream.end(()=>{
                        return resolve()
                    })
                }else{
                    return resolve()
                }
            })
            readstream.on('error',(err)=>{
                return reject(new Error(`failed to read cluster file ${err.message}`))
            })
        })
    }

    Query(databasename, clustername, QUERY_FUNCTION){
        return new Promise((resolve,reject)=>{
            const clusterpath = path.join(databasename,`${clustername}.json`)
            if (!fs.existsSync(clusterpath)){
                return reject(new Error('cluster does not exists in the database'))
            }
            const readstream = fs.createReadStream(clusterpath,{encoding:'utf8'})
            let datastr = ''
            readstream.on('data',(chunk)=>{
                datastr += chunk
            })
            readstream.on('end',()=>{
                const read = JSON.parse(datastr)
                const queries = read.filter(QUERY_FUNCTION)
                resolve(queries)
            })
            readstream.on('error',err=>{
                reject(new Error(`Error reading data : ${err}`))
            })
        })
    }
    update(databasename,clustername,QUERY_FUNCTION,updatedData){
        return new Promise((resolve, reject)=>{
            const clusterpath = path.join(databasename,`${clustername}.json`)

            if (!fs.existsSync(clusterpath)){
                return reject(new Error('cluster does not exists in the database'))
            }

            const readstream = fs.createReadStream(clusterpath,{encoding:'utf8'})

            let datastr = ''

            readstream.on('data',(chunk)=>{
                datastr += chunk
            })

            readstream.on('end',()=>{
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
                    reject({error : err})
                })
            })
            readstream.on('error',err=>{
                reject({error:err})
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
              let read = JSON.parse(readData);
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

    deleteDuplicates(databaseName, clustername, compareFunction){
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
              let rows = JSON.parse(readData);
              const uniqueRows = [];
        
              rows.forEach(row => {
                if (!uniqueRows.some(existingRow => compareFunction(existingRow, row))) {
                  uniqueRows.push(row);
                }
              });
        
              const writeStream = fs.createWriteStream(clusterpath);
              writeStream.write(JSON.stringify(uniqueRows));
              writeStream.end();
        
              writeStream.on('finish', () => {
                resolve('Duplicate data deleted successfully');
              });
        
              writeStream.on('error', error => {
                reject(new Error(`Error deleting duplicate data: ${error.message}`));
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
                reject(new Error('Cluster does not exist'));
            }
    
            const readStream = fs.createReadStream(clusterpath);
            let readData = '';
    
            readStream.on('data', chunk => {
                readData += chunk;
            });
    
            readStream.on('end', () => {
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
            });
    
            readStream.on('error', error => {
                reject(new Error(`Error reading data: ${error}`));
            });
        });
    }


}


const db = new Database()

// creating the database
// db.createDatabase('mycustomers').then(()=>{
//     console.log('database created successfully');
// }).catch((err)=>{
//     console.log(err);
// })

// creating the cluster
// db.createCluster('mycustomers','users').then(()=>{
//     console.log('cluster created');
// }).catch((err)=>{
//     console.log(err);
// })

// inserting data into the database
// db.insert('mycustomers','users',{name: 'shashank',age:21,phonenumber:9876543210,gender:'male'},false).then(()=>{
//     console.log('data inserted successfully');
// }).catch((err)=>{
//     console.log(err);
// })

// Query the data cluster:
// db.Query('mycustomers','users',(data)=>data)
// .then((data)=>{
//     console.log(data);
// }).catch((err)=>{
//     console.log(err);
// })

// Update the data : 
// db.update('mycustomers','users',(data)=>data.name==='shashank',{email : 'shashank@mail.com'}).then(()=>console.log('data updated')).catch((err)=>console.log(err))

// delete data:
// db.delete('mycustomers','users',(data)=>data.name === 'shashank').then(()=>{
//     console.log('data deleted sucessfully');
// }).catch((err)=>{
//     console.log(err);
// })

// delete duplicates:
// db.deleteDuplicates('mycustomers','users',(data)=> data.name === 'shashank').then(()=>{
//     console.log('dups deleted');
// }).catch((err)=>{
//     console.log(err);
// })

// db.search('mycustomers','users','sh').then((data)=>{
//     console.log(data);
// }).catch((err)=>{
//     console.log(err);
// })
