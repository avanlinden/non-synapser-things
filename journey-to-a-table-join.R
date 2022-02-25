### Playing with table joins/materialized views in Synapse staging API ###

library(here)

# use the `schemann` package to set api endpoints
remotes::install_github("Sage-Bionetworks/schemann")
library(schemann)

# load reticulate + synapseclient
library(reticulate)
synapse <- reticulate::import("synapseclient")
syn <- synapse$Synapse()

# load synapseutils
synapseutils <- reticulate::import("synapseutils")

# login to synapse
syn$login()

# set staging endpoints w/ schemann
set_synapse_endpoints(syn, staging = TRUE)

# use restPOST to create view in synapse


# create test project on Synapse Staging, store synID here
project_id <- "syn27240565"

# make a new folder with API calls to challenge myself:

# create post body to make a new folder in this project
folder_body <- '{"concreteType":"org.sagebionetworks.repo.model.Folder", "name":"Data Files", "parentId": "syn27240565"}'

# post it
syn$restPOST(uri = "/entity",
             body = folder_body)

# store synID of the folder I just made
folder_id <- "syn27240751"

### Test drive materialized view -- make a dummy fileview of "data", then join to a table ------

# make 3 dummy text files for fileview
writeLines("Fake patient 3 sequence data", here("dummy-files/patient3.txt"))

# make a manifest with some annotations to bulk upload those files
path <- list.files(here("dummy-files/"), full.names = TRUE)
parent <- rep(folder_id, 3)
individualID <- c("patient1", "patient2", "patient3")
specimenID <- c("patient1_rnaseq", "patient2_rnaseq", "patient3_rnaseq")
sex <- c("male", "female", "female")
diagnosis <- c("chill", "cool", "ice-cold")

manifest <- data.frame(path, parent, individualID, specimenID, sex, diagnosis)

write_tsv(manifest, file = here("dummy-files/manifest.tsv"))

# upload files via syncToSynapse with manifest

synapseutils$syncToSynapse(syn = syn, 
                           manifestFile = here("dummy-files/manifest.tsv"))

#ok, let's make a fileview scoped to those files
view_schema <- synapseclient$table$EntityViewSchema(name = "Patient Data Fileview",
                                             parent = project_id,
                                             scopes = folder_id,
                                             addAnnotationColumns = TRUE)

syn$store(view_schema)

# let's make some metadata tables
patient_metadata <- data.frame(individualID = c("patient1", "patient2", "patient3", "patient4"),
                               sex = c("male", "female", "female", NA_character_),
                               diagnosis = c("chill", "cool", "ice-cold", "flamin' hot"),
                               height = c(1.6, 1.4, 1.25, 2.01),
                               pet = c("gecko", "hamster", "dog", "dog"))

specimen_metadata <- data.frame(individualID = c("patient1", "patient1", "patient2", "patient2", "patient3", "patient4"),
                                specimenID = c("patient1_rnaseq", "patient1_lipid", "patient2_rnaseq", "pateient2_lipid", "patient3_rnaseq", "patient4_lipid"),
                                tissue = c("blood", "blood", "cerebellum", "serum", "cerebellum", "blood"),
                                assay = c("rnaSeq", "metabolon", "rnaSeq", "metabolon", "rnaSeq", "nightingale"),
                                postmortem = c(F, F, F, T, T, F)
)
                                
# store tables to synapse
patient_table <- synapseclient$table$build_table("Individual Metadata", project_id, patient_metadata)
stored_patient_table <- syn$store(patient_table)

specimen_table <- synapseclient$table$build_table("Specimen Metadata", project_id, specimen_metadata)
stored_specimen_table <- syn$store(specimen_table)

# MATERIALIZED VIEW TIME!!!!

# example materialized view body from confluence documentation:
# {
#   "concreteType":"org.sagebionetworks.repo.model.table.MaterializedView",
#   "name":"My First MaterializedView",
#   "parentId":"syn111",
#   "definingSQL":"SELECT * FROM syn222 F JOIN syn444 P on (F.patientId = P.patientId) WHERE P.age > 60"
# }

# construct the body of the API call
mat_view_body <- '{"concreteType":"org.sagebionetworks.repo.model.table.MaterializedView", "name":"Test Materialized View 4","parentId":"syn27240565","definingSQL":"SELECT * FROM syn27241292 Files JOIN syn27241639 Metadata on (Files.individualID = Metadata.individualID)"}'

# use restPOST to create materialized view
mat_view <- syn$restPOST(uri = "/entity",
             body = mat_view_body)

# get synID of the newly created view
mat_view_id <- mat_view$id

## TODO: figure out how to use "POST /entity/syn999/table/query/async/start" to query a table/view

# query the materialized view using the regular python client function

mat_view_query <- syn$tableQuery("select * from syn27242117")

mat_view_df <- mat_view_query$asDataFrame()

# it's a thing!
mat_view_df

# store the materialized view data back as a regular Synapse table so it can be viewed in the web UI as an example
mat_view_illustration <- synapseclient$table$build_table("Materialized View Example", project_id, mat_view_df)
syn$store(mat_view_illustration)
