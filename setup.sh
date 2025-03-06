#!/bin/bash

# Script to create and setup a Python virtual environment
# Note: This script should be sourced, not executed: source setup.sh

# Set the name of the virtual environment (change if desired)
VENV_NAME=".venv"

echo "Setting up virtual environment '$VENV_NAME'..."

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed. Please install Python 3 first."
    return 1 2>/dev/null || exit 1  # return if sourced, exit otherwise
fi

# Create the virtual environment
echo "Creating virtual environment..."
python3 -m venv $VENV_NAME

# Check if creation was successful
if [ ! -d "$VENV_NAME" ]; then
    echo "Error: Failed to create virtual environment."
    return 1 2>/dev/null || exit 1  # return if sourced, exit otherwise
fi

# Activate the virtual environment and install dependencies
echo "Installing dependencies from requirements.txt..."
source $VENV_NAME/bin/activate

# Check if requirements.txt exists
if [ ! -f "requirements.txt" ]; then
    echo "Warning: requirements.txt not found in the current directory."
    echo "Virtual environment created but no packages were installed."
else
    pip install --upgrade pip
    pip install -r requirements.txt
    
    # Check if installation was successful
    if [ $? -eq 0 ]; then
        echo "Dependencies installed successfully."
    else
        echo "Error: Failed to install some dependencies."
    fi
fi

echo ""
echo "Virtual environment setup complete and activated!"
echo "To deactivate when finished, run: deactivate"

# Keep the environment active for the user
# No need to tell them to activate it since it's already active
