import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
import matplotlib.pyplot as plt

def show_pattern(uid_names, uid_dfs, column_name):
    # Calculate the number of plots
    num_plots = len(uid_dfs)
    
    # Calculate the number of rows (each row will have two subplots)
    # num_rows = (num_plots + 1) // 2  # Add 1 and use integer division to ensure at least two rows
    
    # Create subplots with two columns and the calculated number of rows
    fig, axes = plt.subplots(num_plots, 1, figsize=(12, 6 * num_plots))
    
    # Flatten the 2D array of axes
    axes = axes.flatten()
    
    # Plot each DataFrame on a subplot
    for idx, (df, ax) in enumerate(zip(uid_dfs, axes)):
        # Plot each activity on the y-axis
        for col in column_name:
            # check missing column
            if col not in df.columns:
                continue
            # check missing value
            df2 = df.loc[df[col]!= '']
            ax.plot(df2.index, df2[col], marker='o', label=col)

        # Customize the plot for each subplot
        ax.set_xlabel('Datetime')
        ax.set_ylabel('Activity')
        ax.set_title(f'{uid_names[idx]} Activities Over Time')
        ax.grid(True)
        ax.legend()

        # Rotate x-axis labels for better readability (optional)
        ax.tick_params(axis='x', rotation=45)

    # Remove any empty subplots (if the number of plots is not a multiple of 2)
    if num_plots % 2 == 1:
        fig.delaxes(axes[-1])

    # Show the plot
    plt.tight_layout()
    plt.show()
