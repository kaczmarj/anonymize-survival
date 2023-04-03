import json
from pathlib import Path
import sys

import click
import pandas as pd

sys.tracebacklimit = 0


def get_dt_diagnosis(row, col_dod_yyyy: str, col_dod_mm: str, col_dod_dd: str):
    y = row[col_dod_yyyy]
    m = row[col_dod_mm]
    d = row[col_dod_dd]
    return f"{y}-{m}-{d}"


def get_dt_last_contact(
    row,
    col_dolc_yyyy: str,
    col_dolc_mm: str,
    col_dolc_dd: str,
):
    y = row[col_dolc_yyyy]
    m = row[col_dolc_mm]
    d = row[col_dolc_dd]
    return f"{y}-{m}-{d}"


@click.command()
@click.option(
    "--inpath",
    type=click.Path(exists=True),
    required=True,
    help="Path to input dataset with dates of diagnosis, dates of last contact, and"
    " vital status",
)
@click.option(
    "--outpath",
    type=click.Path(exists=False),
    required=True,
    help="Path to output file (with .csv extension)",
)
@click.option(
    "--col-index", default="STUDYID", help="Column name of anonymous patient identifier"
)
@click.option(
    "--col-dod-yyyy",
    default="D_DATE_OF_DIAGNOSIS_YYYY",
    show_default=True,
    help="Column name of date of diagnosis year",
)
@click.option(
    "--col-dod-mm",
    default="D_DATE_OF_DIAGNOSIS_MM",
    show_default=True,
    help="Column name of date of diagnosis month",
)
@click.option(
    "--col-dod-dd",
    default="D_DATE_OF_DIAGNOSIS_DD",
    show_default=True,
    help="Column name of date of diagnosis day",
)
@click.option(
    "--col-dolc-yyyy",
    default="D_DATE_OF_LAST_CONTACT_YYYY",
    show_default=True,
    help="Column name of date of last contact year",
)
@click.option(
    "--col-dolc-mm",
    default="D_DATE_OF_LAST_CONTACT_MM",
    show_default=True,
    help="Column name of date of last contact month",
)
@click.option(
    "--col-dolc-dd",
    default="D_DATE_OF_LAST_CONTACT_DD",
    show_default=True,
    help="Column name of date of last contact day",
)
@click.option(
    "--col-vital-status",
    default="D_VITAL_STATUS",
    show_default=True,
    help="Column name of vital status",
)
@click.option(
    "--vital-status-value-alive",
    default="Alive",
    show_default=True,
    help="Value for vital status == alive",
)
@click.option(
    "--vital-status-value-deceased",
    default="Dead",
    show_default=True,
    help="Value for vital status == deceased",
)
@click.option(
    "--filetype",
    type=click.Choice(["csv", "excel", "sas"]),
    default="excel",
    show_default=True,
    help="The file type of the input file",
)
@click.option(
    "--pandas-reader-arguments",
    default="{}",
    show_default=True,
    help="Keyword arguments to pass to Pandas read_FILETYPE function. Format this as a"
    " JSON string.",
)
def main(
    *,
    inpath: str,
    outpath: str,
    col_index: str,
    col_dod_yyyy: str,
    col_dod_mm: str,
    col_dod_dd: str,
    col_dolc_yyyy: str,
    col_dolc_mm: str,
    col_dolc_dd: str,
    col_vital_status: str,
    vital_status_value_alive,
    vital_status_value_deceased,
    filetype: str,
    pandas_reader_arguments: str,
):
    """Create CSV with relative days of survival and censoring."""

    if Path(outpath).exists():
        click.secho(f"Output path exists: {outpath}", fg="red")
        sys.exit(1)

    pandas_reader_arguments = json.loads(pandas_reader_arguments)
    filetype = filetype.lower()
    df: pd.DataFrame
    if filetype == "csv":
        df = pd.read_csv(inpath, **pandas_reader_arguments)
    elif filetype == "excel":
        df = pd.read_excel(inpath, **pandas_reader_arguments)
    elif filetype == "sas":
        df = pd.read_sas(inpath, **pandas_reader_arguments)
    else:
        raise NotImplementedError(
            "Please choose a supported filetype or contact the developer."
            " <jakub.kaczmarzyk@stonybrookmedicine.edu>"
        )

    # Test that columns are present in the data.
    expected_cols = [
        col_index,
        col_dod_yyyy,
        col_dod_mm,
        col_dod_dd,
        col_dolc_yyyy,
        col_dolc_mm,
        col_dolc_dd,
        col_vital_status,
    ]
    expected_cols = pd.Index(expected_cols)
    mask = expected_cols.isin(df.columns)
    if not mask.all():
        missing_cols = expected_cols[~mask]
        click.secho(
            "The following columns are missing from the data table:"
            f"{missing_cols.tolist()}"
            "\n\nPlease check your column names or email the developer,"
            " Jakub Kaczmarzyk <jakub.kaczmarzyk@stonybrookmedicine.edu>.",
            fg="red",
        )
        sys.exit(1)

    # Check if vital status column contains the alive / deceased values.
    if vital_status_value_alive not in set(df[col_vital_status]):
        click.secho(
            f"The vital status column '{col_vital_status}' does not contain the value"
            f" indicating alive '{vital_status_value_alive}'.",
            fg="red",
        )
        sys.exit(1)
    if vital_status_value_deceased not in set(df[col_vital_status]):
        click.secho(
            f"The vital status column '{col_vital_status}' does not contain the value"
            f" indicating deceased '{vital_status_value_deceased}'.",
            fg="yellow",
        )

    print("Vital status counts before clipping dates:")
    print(df[col_vital_status].value_counts())

    dates_diagnosis = df.apply(
        get_dt_diagnosis,
        axis=1,
        col_dod_yyyy=col_dod_yyyy,
        col_dod_mm=col_dod_mm,
        col_dod_dd=col_dod_dd,
    )
    df["date_of_diagnosis_yyyymmdd"] = pd.to_datetime(
        dates_diagnosis, format="%Y-%m-%d", errors="coerce"
    )
    del dates_diagnosis

    dates_last_contact = df.apply(
        get_dt_last_contact,
        axis=1,
        col_dolc_yyyy=col_dolc_yyyy,
        col_dolc_mm=col_dolc_mm,
        col_dolc_dd=col_dolc_dd,
    )
    df["date_of_last_contact_yyyymmdd"] = pd.to_datetime(
        dates_last_contact, format="%Y-%m-%d", errors="coerce"
    )
    del dates_last_contact

    print("Number of samples", len(df))
    print("Dropping rows where datetime coercion failed")
    df = df.dropna(
        subset=["date_of_diagnosis_yyyymmdd", "date_of_last_contact_yyyymmdd"]
    ).copy()
    print("Number of samples", len(df))

    END_DATE = pd.to_datetime("2020-12-31", format="%Y-%m-%d")
    print("Using study end date", END_DATE)

    diagnosed_prior_to_study_end = df["date_of_diagnosis_yyyymmdd"] <= END_DATE

    print("Modifying vital status of deceased who had last contact after", END_DATE)
    df["vital_status_at_study_end"] = df[col_vital_status].copy()
    last_contact_after_study_end = df["date_of_last_contact_yyyymmdd"] > END_DATE
    deceased = df[col_vital_status] == vital_status_value_deceased
    deceased_overall_but_alive_at_study_end = last_contact_after_study_end & deceased
    df.loc[
        deceased_overall_but_alive_at_study_end, "vital_status_at_study_end"
    ] = vital_status_value_alive
    print("Clipping date of last contact to", END_DATE)
    df["date_of_last_contact_clipped_yyyymmdd"] = df[
        "date_of_last_contact_yyyymmdd"
    ].clip(upper=END_DATE)

    print(
        "Keeping",
        diagnosed_prior_to_study_end.sum(),
        "patients who were diagnosed prior to",
        END_DATE,
    )
    print("Original data had", len(df), "patients")
    df = df.loc[diagnosed_prior_to_study_end, :].copy()

    # Survival in days.
    print("Calculating relative survival in days...")
    df["survivalA"] = (
        df["date_of_last_contact_clipped_yyyymmdd"] - df["date_of_diagnosis_yyyymmdd"]
    )
    df["survivalA"] = df["survivalA"].dt.days

    # Censor column is 0=alive, 1=deceased.
    print("Creating censor column...")
    df["censorA.0yes.1no"] = df["vital_status_at_study_end"].replace(
        {vital_status_value_alive: 0, vital_status_value_deceased: 1}
    )

    cols = [col_index, "censorA.0yes.1no", "survivalA"]
    print("Saving data with columns", cols)
    print(f"  {len(df)} samples")
    click.secho(f"Saving to {outpath}", fg="green")
    df[cols].to_csv(outpath, index=False)


if __name__ == "__main__":
    main()
