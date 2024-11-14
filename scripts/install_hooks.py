import logging
import os
import shutil
from pathlib import Path


def main():
    root_dir = Path(__file__).parent.parent
    hooks_dir = root_dir / ".git" / "hooks"
    hooks_dir.mkdir(parents=True, exist_ok=True)

    pre_commit_source = root_dir / "hooks" / "pre-commit"
    pre_commit_dest = hooks_dir / "pre-commit"

    shutil.copy(pre_commit_source, pre_commit_dest)
    os.chmod(pre_commit_dest, 0o755)  # noqa: S103  # Standard permission for executable scripts

    logger = logging.getLogger(__name__)
    logger.info("Git hooks installed successfully.")


if __name__ == "__main__":
    main()
