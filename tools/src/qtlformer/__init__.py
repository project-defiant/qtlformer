"""QTL manifest preparation tool."""

from __future__ import annotations
import typer
from fsspec import filesystem
import logging
from typing import Annotated
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import re
from typing import Self
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_path(value: str) -> str:
    if not isinstance(value, str) or value == "":
        raise typer.BadParameter("path must be valid path")
    return value


def validate_project_id(value: str) -> str:
    logger.debug(f"Validating project ID: {value}")
    if not isinstance(value, str) or value == "":
        raise typer.BadParameter("GCP project ID must be provided.")
    return value


class DatasetOrStudyNameError(Exception):
    pass


def validate_name(name: str, pattern: str) -> str:
    logger.debug(f"Validating dataset name: {name}")
    if not isinstance(name, str) or name == "":
        raise DatasetOrStudyNameError("Name has to be a non-empty string.")

    _match = re.compile(pattern).fullmatch(name)
    if not _match:
        raise DatasetOrStudyNameError(
            f"Name '{name}' does not match pattern '{pattern}'."
        )

    return name


@dataclass
class QTLDataset:
    """QTL Dataset representation.

    This class represents a single QTL dataset within a study, including paths to
    its associated SuSiE credible sets and lbf variable parquet files.

    NOTE: The paths are relative to the base path of the dataset.
    """

    id: str
    """Identifier of the QTL dataset."""
    susie_cs_path: str
    """Relative path to the SuSiE credible sets parquet file."""
    susie_lbf_path: str
    """Relative path to the SuSiE lbf variable parquet file."""

    @staticmethod
    def _validate_name(name: str) -> str:
        logger.debug(f"Validating dataset name: {name}")
        return validate_name(name, r"^QTD\d+$")

    @classmethod
    def from_path(cls, path: str) -> QTLDataset | None:
        try:
            # NOTE: Assuming path structure is .../{relative_base_path}/{study_id}/{dataset_id}
            dataset_id = cls._validate_name(path.split("/")[-1])
        except DatasetOrStudyNameError:
            return None

        susie_cs_path = f"{path}/{dataset_id}.credible_sets.parquet"
        susie_lbf_path = f"{path}/{dataset_id}.lbf_variable.parquet"
        logger.info(
            f"Checking existence of SuSiE files for dataset '{dataset_id}': "
            f"CS path: {susie_cs_path}, LBF path: {susie_lbf_path}"
        )
        fs = filesystem("local")
        if not fs.exists(susie_cs_path) or not fs.exists(susie_lbf_path):
            logger.warning(
                f"Dataset '{dataset_id}' is missing required SuSiE files. Skipping."
            )
            return None

        # Transform to relative paths to the input_path provided by the user
        logger.info("Transforming SuSiE paths relative to INPUT-PATH")
        susie_cs_path = "/".join(susie_cs_path.split("/")[-3:])
        susie_lbf_path = "/".join(susie_lbf_path.split("/")[-3:])
        return cls(
            id=dataset_id,
            susie_cs_path=susie_cs_path,
            susie_lbf_path=susie_lbf_path,
        )


@dataclass
class QTLStudy:
    id: str
    path: str
    datasets: list[QTLDataset]

    @classmethod
    def from_path(cls, path: str) -> QTLStudy:
        study_id = cls._validate_name(path.split("/")[-1])
        datasets = []
        return cls(id=study_id, path=path, datasets=datasets)

    @staticmethod
    def _validate_name(name: str) -> str:
        logger.debug(f"Validating study name: {name}")
        return validate_name(name, r"^QTS\d+$")

    def get_datasets(self) -> Self:
        fs = filesystem("local")
        dataset_paths = fs.ls(self.path)
        datasets = [QTLDataset.from_path(p) for p in dataset_paths]
        datasets = [ds for ds in datasets if ds is not None]
        self.datasets = datasets
        return self


@dataclass
class QTLManifest:
    studies: list[QTLStudy]

    def __post_init__(self):
        logger.debug(f"Initialized QTLManifest with {len(self.studies)} studies.")
        self.df = self.transform()

    @staticmethod
    def from_path(path: str) -> QTLManifest:
        fs = filesystem("local")
        studies: list[QTLStudy] = []
        study_paths = fs.ls(path)

        def _prepare_study_for_path(p: str) -> None | QTLStudy:
            try:
                study = QTLStudy.from_path(p).get_datasets()
                return study
            except DatasetOrStudyNameError:
                logger.warning(f"Skipping invalid study path '{p}'local")
                return None

        logger.info(f"Found {len(study_paths)} blobs in {path}.")
        th = ThreadPoolExecutor(max_workers=5)
        result = list(th.map(_prepare_study_for_path, study_paths))
        studies = [study for study in result if study is not None]
        return QTLManifest(studies=studies)

    def transform(self) -> pd.DataFrame:
        logger.info("Transforming manifest to DataFrame...")
        records = []
        for study in self.studies:
            for dataset in study.datasets:
                records.append(
                    {
                        "study_id": study.id,
                        "dataset_id": dataset.id,
                        "susie_cs_path": dataset.susie_cs_path,
                        "susie_lbf_path": dataset.susie_lbf_path,
                    }
                )
        import pandas as pd

        df = pd.DataFrame.from_records(records)
        return df

    def log_statistics(self) -> None:
        logger.info("Logging manifest statistics...")
        logger.info(f"Total studies: {len(self.studies)}")
        logger.info(self.df)

    def to_parquet(self, output_path: str) -> None:
        logger.info(f"Writing manifest to {output_path} in Parquet format.")
        fs = filesystem("local")
        with fs.open(output_path, "wb") as f:
            self.df.to_csv(f, index=False, sep="\t", header=True)
        logger.info("Manifest successfully written.")


cli = typer.Typer(no_args_is_help=True)


@cli.command()
def manifest(
    input_path: Annotated[str, typer.Argument(callback=validate_path)],
    output_path: Annotated[str, typer.Argument(callback=validate_path)],
) -> None:
    """Prepare QTL manifest from path and save as parquet."""

    logger.info("Starting QTL manifest preparation.")
    man = QTLManifest.from_path(input_path)
    man.log_statistics()
    man.to_parquet(output_path)
    logger.info("Reading blobs from source bucket.")


@cli.command()
def susie_to_study_locus(
    susie_path: Annotated[str, typer.Argument(callback=validate_path)],
    study_locus_path: Annotated[str, typer.Argument(callback=validate_path)],
) -> None:
    """Harmonise SuSiE QTL data from path and save as parquet."""

    logger.info("Starting SuSiE QTL harmonisation.")


@cli.command()
def manifest_to_study_index(
    manifest_path: Annotated[str, typer.Argument(callback=validate_path)],
    metadata_path: Annotated[str, typer.Argument()],
    study_index_path: Annotated[str, typer.Argument(callback=validate_path)],
) -> None:
    """Convert QTL manifest from path to study index parquet."""

    logger.info("Starting QTL manifest to study index conversion.")
