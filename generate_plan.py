import uvicorn
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

test_update = [8, 9, 10, 11, 12, 13, 17, 18, 14, 19]
rc_update = [7, 9, 10, 12, 13, 15]
ftp_folder = [7, 9, 10, 12, 13, 15, 14]
staging = [7, 9, 10, 12, 13, 15, 16]
archive_folder = [7, 9, 10, 12, 13, 15, 16, 18, 17]

hu_db_version = 17
prod_bin_version = 0

min_version = 0
max_version = 0

result = {}
versions = set()


def highest(folder, is_primary):
    global min_version, max_version
    max_update_version = max(folder) if folder else 0
    function_result = ([max_update_version] if max_update_version > max(min_version, max_version) else [])
    if is_primary and function_result:
        max_version = max_update_version
    return function_result


def full_list(folder, is_primary):
    global min_version, max_version

    def check_items(sub_function_folder, l_bound, u_bound):
        sub_function_result = []
        for folder_item in sub_function_folder:
            if l_bound < folder_item <= u_bound:
                sub_function_result.append(folder_item)
        return sub_function_result

    function_result = []
    if is_primary:
        function_result = check_items(folder, min_version, min_version + 1000)
        if function_result:
            max_version = max(function_result)
    elif max_version > min_version:
        function_result = check_items(folder, min_version, max_version)

    return function_result


def copy_update(upd_lst):
    global result, versions
    overwrite = upd_lst['overwrite']
    folder_type = upd_lst['primary']
    update_type = upd_lst['update_type']
    folder_name = upd_lst['folder_name']
    folder_path = upd_lst['folder_path']
    file_prefix = upd_lst['file_prefix']
    function = upd_lst["search_type"]
    folder = upd_lst['folder']
    temp_folder = function(folder, folder_type)
    for item in temp_folder:
        versions.add(item)
        if (result.get(item, None) is None) or overwrite:
            filename = f"{file_prefix}{item}.zip"
            result[item] = {"filename": filename, "folder": folder_name, "update_type": update_type, "folder_path": folder_path}


def main_function(f_priority):
    global result, versions, hu_db_version, prod_bin_version, min_version, max_version

    result = {}
    max_version = 0
    versions = set()
    min_version = min(hu_db_version, prod_bin_version) if prod_bin_version else hu_db_version
    max_inputted_version = max(hu_db_version, prod_bin_version)

    for item in f_priority:
        copy_update(item)

    success = False
    result_description = ""
    prod_pp_text = f" and prod_bin_version {prod_bin_version}" if prod_bin_version else ""

    if max_version:
        if max_version - min_version == len(versions):
            if max_inputted_version <= max_version:
                success = True
                result_description = f"Generated plan (for hu_db_version {hu_db_version}{prod_pp_text}) contains {len(versions)} updates. Max version is {max_version}"
            else:
                if max_inputted_version == prod_bin_version:
                    result_description = f"Max version of update files (v {max_version}) is less than deployed parameter version on production (v {prod_bin_version}). Missing update versions are: {[i for i in range(max_version + 1, prod_bin_version + 1)]}"
                else:
                    result_description = f"Max version of update files (v {max_version}) is less than database version on Hot Update environment (v {hu_db_version}). Missing update versions are: {[i for i in range(max_version + 1, hu_db_version + 1)]}"
        else:
            missing_versions = []
            for l in range(min_version + 1, max_version):
                if l not in versions:
                    missing_versions.append(l)
            result_description = f"Download plan (for hu_db_version {hu_db_version}{prod_pp_text}) can't be generated. {len(missing_versions)} update files (versions: {str(missing_versions)}) are missing"
    else:
        result_description = f"No update files were found with versions > {min_version}"


    output = {"success": success, "description": result_description, "plan": result}
    return output

folder_priority = [
    {
        "folder": rc_update,
        "folder_name": "rc_update",
        "folder_path": "https://hu.s3.amazonaws.com/rc_update/",
        "primary": True,
        "search_type": full_list,
        "overwrite": True,
        "update_type": "rc",
        "file_prefix": "UPD-"
    },
    {
        "folder": test_update,
        "folder_name": "test_update",
        "folder_path": "https://hu.s3.amazonaws.com/test_update/",
        "primary": True,
        "search_type": highest,
        "overwrite": False,
        "update_type": "test",
        "file_prefix": "UPD-"
    },
    {
        "folder": ftp_folder,
        "folder_name": "ftp_folder",
        "folder_path": "https://hu.s3.amazonaws.com/ftp_folder/",
        "primary": False,
        "search_type": full_list,
        "overwrite": False,
        "update_type": "rc",
        "file_prefix": "UPD-"
    },
    {
        "folder": staging,
        "folder_name": "staging",
        "folder_path": "https://hu.s3.amazonaws.com/staging/",
        "primary": False,
        "search_type": full_list,
        "overwrite": False,
        "update_type": "rc",
        "file_prefix": "UPD-"
    },
    {
        "folder": archive_folder,
        "folder_name": "archive_folder",
        "folder_path": "https://hu.s3.amazonaws.com/archive_folder/",
        "primary": False,
        "search_type": full_list,
        "overwrite": False,
        "update_type": "rc",
        "file_prefix": "UPD-"
    }
]


@app.get("/")
def read_root():
    return main_function(folder_priority)


@app.get("/{number}")
def read_item(number: str):
    global hu_db_version
    try:
        hu_db_version = int(number)
    except:
        pass
    return main_function(folder_priority)


@app.get("/prod-bin-version/{number}")
def update_prod_bin_version(number: str):
    global prod_bin_version
    try:
        value = int(number)
        if value > 0:
            prod_bin_version = int(number)
        else:
            prod_bin_version = 0
    except:
        pass
    return {"hu_db_version": hu_db_version, "prod_bin_version": prod_bin_version if prod_bin_version else "Not set"}


@app.get("/prod-bin-version/")
def view_prod_bin_version():
    return {"hu_db_version": hu_db_version, "prod_bin_version": prod_bin_version if prod_bin_version else "Not set"}


@app.get("/full-inputs/")
def read_full_inputs():
    return folder_priority


@app.get("/inputs/")
def read_inputs():
    output = {}
    for i in folder_priority:
        output[i["folder_name"]] = str(i["folder"])
    return output


@app.post("/inputs/")
async def root(request: Request):
    correct = True
    try:
        body = (await request.json())
    except:
        raise HTTPException(status_code=400, detail="Incorrect request body")
    for k, v in body.items():
        if not isinstance(v, list):
            correct = False
        else:
            for p in v:
                if not isinstance(p, int):
                    correct = False
        if not correct:
            raise HTTPException(status_code=400, detail="Incorrect request body")

        for i in folder_priority:
            if i["folder_name"] == k:
                i["folder"] = v

    return read_inputs()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
