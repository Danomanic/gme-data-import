require('dotenv').config();
const neatCsv = require('neat-csv');
const fs = require('fs');
const util = require('util');
const crypto = require('crypto');
const { MongoClient } = require('mongodb');

const client = new MongoClient(process.env.MONGO_URL, { useNewUrlParser: true });

const importFolder = './import/';

const readDir = util.promisify(fs.readdir);
const readFile = util.promisify(fs.readFile);

async function load() {
  const files = await readDir(importFolder);
  const file = await readFile(`${importFolder}${files[0]}`);
  const data = await neatCsv(file, { separator: ',', skipLines: 3 });
  return data;
}

async function parseMembers(data) {
  const members = new Set();
  data.forEach((item) => {
    const tempMember = {
      id: item.contact_number1,
      first_name: item.preferred_forename1,
      last_name: item.surname1,
      email: item.Email1,
    };
    if (!members.has(JSON.stringify(tempMember))) members.add(JSON.stringify(tempMember));
  });
  return new Set([...members].map((o) => JSON.parse(o)));
}

async function parseRoles(data) {
  const roles = new Set();
  data.forEach((item) => {
    const temp = {
      role_id: crypto.createHash('md5').update(`${item.contact_number1}/${item.MRole1}/${item.RoleStatus1}`).digest('hex'),
      member_id: item.contact_number1,
      name: item.MRole1,
      status: item.RoleStatus1,
      start: item.Role_Start_Date1,
      review: item.review_date1,
      woodbadge: item.wood_received1,
      county: item.County1,
      county_section: item.County_Section1,
      district: item.District1,
      district_section: item.District_Section1,
      group: item.Scout_Group1,
      group_section: item.Scout_Group_Section1,
    };
    if (!roles.has(JSON.stringify(temp))) roles.add(JSON.stringify(temp));
  });
  return new Set([...roles].map((o) => JSON.parse(o)));
}

async function parseModules(data) {
  const modules = new Set();
  data.forEach((item) => {
    const tempModule = {
      member_id: item.contact_number1,
      role_module_id: crypto.createHash('md5').update(`${item.contact_number1}/${item.MRole1}/${item.RoleStatus1}/${item.module_name1}`).digest('hex'),
      role_id: crypto.createHash('md5').update(`${item.contact_number1}/${item.MRole1}/${item.RoleStatus1}`).digest('hex'),
      name: item.module_name1,
      validated_date: item.module_validated_date1,
      validated_by: item.validated_by1,
      validated_by_name: item.Validatedbyname,
      learning_method_description: item.learning_method_description,
      learning_method_actual_completion: item.learningmethod_actual_completion,
      validation_criteria: item.Validation_criteria,
      validation_criteria_actual_completion: item.VCactual_completion,
      validation_method: item.VMname,
      training_advisor_id: item.training_advisor_number1,
      training_advisor_name: item.trainingadvisorname,
    };
    if (!modules.has(JSON.stringify(tempModule))) modules.add(JSON.stringify(tempModule));
  });
  return new Set([...modules].map((o) => JSON.parse(o)));
}

function chunkArray(myArray, chunkSize) {
  let index = 0;
  const arrayLength = myArray.length;
  const tempArray = [];

  for (index = 0; index < arrayLength; index += chunkSize) {
    const myChunk = myArray.slice(index, index + chunkSize);
    // Do something if you want with the group
    tempArray.push(myChunk);
  }

  return tempArray;
}

const main = (async () => {
  const data = await load();
  const members = new Set(await parseMembers(data));
  const roles = new Set(await parseRoles(data));
  const modules = new Set(await parseModules(data));
  console.log('Members: ', members.size);
  console.log('Roles: ', roles.size);
  console.log('Modules: ', modules.size);

  client.connect(async (err) => {
    if (err) { process.exit(0); }

    await client.db('scouts').collection('members').deleteMany({});
    await client.db('scouts').collection('roles').deleteMany({});
    await client.db('scouts').collection('modules').deleteMany({});

    // Insert Many Members
    await client.db('scouts').collection('members').insertMany(Array.from(members), (membersErr, res) => {
      if (membersErr) throw membersErr;
      console.log(`Number of members inserted: ${res.insertedCount}`);
    });


    // Insert Many Roles
    await client.db('scouts').collection('roles').insertMany(Array.from(roles), (rolesErr, res) => {
      if (rolesErr) throw rolesErr;
      console.log(`Number of roles inserted: ${res.insertedCount}`);
    });

    const splitModules = await chunkArray(Array.from(modules), 10000);

    splitModules.forEach((moduleSet) => {
      // Insert Many Modules
      client.db('scouts').collection('modules').insertMany(moduleSet, (modulesErr, res) => {
        if (modulesErr) throw modulesErr;
        console.log(`Number of modules inserted: ${res.insertedCount}`);
      });
    });

    await client.close();
  });
});

main();
