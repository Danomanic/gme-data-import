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

function addModule(roleId, moduleId, item) {
  const tempModule = {
    member_id: item.contact_number1,
    module_id: moduleId,
    role_id: roleId,
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
  return tempModule;
}

function addRole(roleId, item) {
  const tempRole = {
    role_id: roleId,
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
    modules: {},
  };
  return tempRole;
}

async function parseMembers(data) {
  const combined = [];
  data.forEach((item) => {
    if (item.contact_number1 !== undefined) {
      const roleId = crypto.createHash('md5').update(`${item.contact_number1}/${item.MRole1}/${item.Role_Start_Date1}`).digest('hex');
      const moduleId = crypto.createHash('md5').update(`${item.contact_number1}/${item.MRole1}/${item.Role_Start_Date1}/${item.module_name1}`).digest('hex');
      if (combined[item.contact_number1] !== undefined) {
        if (combined[item.contact_number1].roles[roleId] !== undefined) {
          combined[item.contact_number1].roles[roleId].modules[moduleId] = addModule(roleId, moduleId, item);
        } else {
          combined[item.contact_number1].roles[roleId] = addRole(roleId, item);
          combined[item.contact_number1].roles[roleId].modules[moduleId] = addModule(roleId, moduleId, item);
        }
      } else {
        const tempModule = {
          member_id: item.contact_number1,
          module_id: moduleId,
          role_id: roleId,
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
        const tempRole = {
          role_id: roleId,
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
          modules: {},
        };
        const tempMember = {
          member_id: item.contact_number1,
          first_name: item.preferred_forename1,
          last_name: item.surname1,
          email: item.Email1,
          roles: {},
        };
        tempMember.roles[roleId] = tempRole;
        tempMember.roles[roleId].modules[moduleId] = tempModule;
        combined[item.contact_number1] = tempMember;
      }
    }
  });
  return combined;
}

const main = (async () => {
  const data = await load();
  let members = await parseMembers(data);

  members = members.filter((el) => el != null);

  let clean = [];

  members.forEach((member) => {
    const tempMember = {
      member_id: member.member_id,
      first_name: member.first_name,
      last_name: member.last_name,
      email: member.email,
      roles: [],
    };

    Object.keys(member.roles).forEach((roleKey) => {
      const role = member.roles[roleKey];
      const tempRole = {
        role_id: role.role_id,
        member_id: role.member_id,
        name: role.name,
        status: role.status,
        start: role.start,
        review: role.review,
        woodbadge: role.woodbadge,
        county: role.county,
        county_section: role.county_section,
        district: role.district,
        district_section: role.district_section,
        group: role.group,
        group_section: role.group_section,
        modules: [],
      };
      Object.keys(role.modules).forEach((moduleKey) => {
        const tmodule = role.modules[moduleKey];
        tempRole.modules.push(tmodule);
      });
      tempMember.roles.push(tempRole);
    });

    clean.push(tempMember);
  });

  client.connect(async (err) => {
    if (err) { process.exit(0); }

    await client.db('scouts').collection('members2').deleteMany({});

    console.log(clean);


    client.db('scouts').collection('members2').insertMany(clean, (membersErr, res) => {
      if (membersErr) throw membersErr;
      console.log(`Number of members inserted: ${res.insertedCount}`);
    });


    await client.close();
  });
});

main();
